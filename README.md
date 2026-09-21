# crmbridge

A small Go service that connects a **shop / Telegram bot**, **Binotel** telephony
and **Nova Poshta** to a **SalesDrive** CRM:

- a site order becomes one CRM deal — however many times the site retries;
- a ringing call shows the caller's card to the responsible manager;
- a missed call becomes a lead, and repeat misses only add a note;
- a Nova Poshta parcel status moves the deal along the funnel and messages the
  customer.

## The actual hard problem

Pushing an order into a CRM is one HTTP call. Keeping the CRM *right* is not:

- **Retries must not duplicate deals.** The site retries a request, the CRM
  create call times out, or the CRM creates the order and the answer is lost.
  The order id is the idempotency key (`externalId`); creation goes through an
  outbox with backoff, and **before any re-create the worker looks the order up
  by `externalId`**, so a lost answer cannot make a second deal.
- **Telephony events repeat and arrive in bursts.** Each call event is stored by
  its call id; the same event again does nothing. A second missed call from the
  same number inside a window adds a note to the existing lead instead of
  spawning a new one. The note may be queued before the lead exists in the CRM;
  it waits for it.
- **Webhooks race the API.** A CRM webhook can arrive before our own create call
  returns; it teaches the bridge the CRM order id early and registers any Nova
  Poshta TTN on the order. Repeated webhooks change nothing.
- **Parcel statuses flap.** A status only counts when it moves *forward*; a
  stale or repeated answer cannot drag a deal back or send "arrived" twice.
  Finished parcels stop being polled; polling is batched (≤100 per request).
- **The CRM is rate limited and sometimes down.** Transient errors are retried
  with exponential backoff, permanent ones (bad key, validation) fail the job at
  once, and a failed job can be requeued after the cause is fixed.

## How it works

```
shop / bot ──POST /site/orders──▶ order (unique id) ──▶ outbox ──▶ SalesDrive /handler/
Binotel ──webhook──▶ ring: ManagerByPhone ──▶ card to the manager (Telegram / log)
                     completed & missed: lead, or a note on a recent lead
SalesDrive ──webhook──▶ order id + Nova Poshta TTN ──▶ tracked parcels
tracker ──▶ Nova Poshta getStatusDocuments (batched) ──▶ forward-only stage change
            ──▶ SalesDrive status update (outbox) + message to the customer
```

`internal/salesdrive` (create order, update status, note, lookup by external id,
manager by phone), `internal/novaposhta` (batched tracking, status → stage),
`internal/binotel` (event parsing), `internal/service` (rules, outbox worker,
tracker), `internal/store` (SQLite, the only state), `internal/api`,
`internal/notify` (Telegram or log), and the stand-ins `internal/fakesalesdrive`,
`internal/fakenp`.

## Try it

Needs Go 1.26, `curl` and `jq`.

```bash
make demo
```

runs the stand-ins and the bridge, then: a shop order sent twice → one deal;
three CRM failures → retried, one deal; a lost answer → found, not duplicated;
a call from a known client → the card goes to their manager; missed calls →
lead, a repeated event ignored, a second miss → a note; a TTN created in the CRM
→ tracked, statuses 5 → 7 → 9 move the deal to the mapped funnel statuses and
the customer gets three messages; a webhook without the secret → 401.
Logs land in `out/`.

| Endpoint | |
|---|---|
| `POST /site/orders` | `{id, source, customer{first_name,last_name,phone,email,telegram_chat}, items[{sku,name,price(kop),qty}], city, warehouse, payment_method, comment, ttn}` |
| `POST /webhooks/binotel?token=` | call events (JSON or form) |
| `POST /webhooks/salesdrive?token=` | SalesDrive order webhook |
| `GET /api/orders/{id}`, `POST /api/ttn`, `GET /api/jobs?state=`, `POST /api/jobs/{id}/requeue` | management |

Configuration: [`examples/config.yaml`](examples/config.yaml) (status map, managers,
lead window, polling, backoff).

## Verification

`make test` (`go test ./... -race`): 25 tests, all passing; statement coverage
66–100% per package (service 81%, SalesDrive client 84%, Nova Poshta 86%).

- one deal for a retried site order; CRM outage retried (`attempts = 3`); lost
  create answer → one `POST /handler/` call and one deal; bad key → failed at
  once with no retries; exhausted retries → `failed`, then requeue → one deal;
- missed calls: lead, duplicate event ignored, note inside the window, new lead
  after it, answered and outgoing calls ignored, note queued before its lead;
- screen pop: unknown caller → default manager, known caller → the responsible
  manager with the client's name;
- parcels: 5 → 7 → 5 → 7 → 9 gives one message per stage and the right funnel
  statuses, a finished parcel is not polled again, refusal alerts the manager,
  250 parcels take exactly 3 Nova Poshta requests (100+100+50), an outage of
  Nova Poshta is retried on the next round;
- webhooks: shared-secret check, form and JSON Binotel shapes, a SalesDrive
  webhook that beats the create answer, repeated webhooks.

Four guards were broken on purpose (no lookup before re-create, no call-event
dedupe, no forward-only rule, no lead window); two survived at first, so the
tests were tightened until each one made a test fail.

## Connecting it to your accounts

- **SalesDrive:** set `salesdrive.base_url` (`https://<account>.salesdrive.me`)
  and `api_key`; add `https://<host>/webhooks/salesdrive?token=<secret>` under
  Webhooks. Put your funnel's status ids in `np_status_map`.
- **Binotel:** send call events to `https://<host>/webhooks/binotel?token=<secret>`;
  map internal numbers to managers in `managers`.
- **Nova Poshta:** with `novaposhta.api_key` set, tracking runs against the
  live API in batches of 100; `tracking.poll_every` sets the pace.
- **Your shop or bot:** `POST /site/orders` with the order id as the
  idempotency key.
- **Another CRM (for example KeyCRM):** implement the five methods of
  `service.CRM` and pass it to `service.New`.
- **Messages:** `telegram_token` sends manager cards and customer statuses to
  Telegram.

## Stack

Go (net/http, log/slog, database/sql), SQLite (`modernc.org/sqlite`, pure Go),
YAML config, Docker.
