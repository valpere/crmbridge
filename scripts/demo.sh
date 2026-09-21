#!/usr/bin/env bash
# End-to-end demo against the local stand-ins. Run `make demo` (needs curl, jq).
set -euo pipefail
cd "$(dirname "$0")/.."

API=http://localhost:8788
SD=http://localhost:9101
NP=http://localhost:9102
TOK="token=demo-secret"
export PATH="$PWD/bin:$PATH"

rm -rf data && mkdir -p data out
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT
fakes >out/fakes.log 2>&1 & pids+=($!)
sleep 0.5
crmbridge -c examples/config.yaml >out/crmbridge.log 2>&1 & pids+=($!)
for _ in $(seq 50); do curl -fs $API/healthz >/dev/null 2>&1 && break; sleep 0.1; done

post() { curl -s -X POST -H 'Content-Type: application/json' "$@"; }
crm()  { curl -s $SD/_orders | jq -c '.[] | {id, externalId, statusId, phone: .data.phone, notes: (.notes | length)}'; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "1. Shop order arrives twice (the site retries): one deal in the CRM"
ORDER='{"id":"web-1001","customer":{"first_name":"Ірина","last_name":"Коваль","phone":"050 123 45 67","telegram_chat":"42"},
  "items":[{"sku":"COF-1","name":"Кава 1 кг","price":12500,"qty":2}],"city":"Київ","warehouse":"12","payment_method":"Оплата при отриманні"}'
post $API/site/orders -d "$ORDER" | jq -c .
post $API/site/orders -d "$ORDER" | jq -c .
sleep 2; crm

step "2. CRM outage: the create call fails 3 times, is retried with backoff, still one deal"
post $SD/_fail -d '{"n":3}'
post $API/site/orders -d '{"id":"web-1002","customer":{"phone":"0671112233"},"items":[{"name":"Чай","price":9900,"qty":1}]}' | jq -c .
sleep 9; crm | grep web-1002

step "3. The CRM creates the order but the answer is lost: found by externalId, not duplicated"
post $SD/_lose -d '{"n":1}'
post $API/site/orders -d '{"id":"web-1003","customer":{"phone":"0931234567"},"items":[{"name":"Кухоль","price":15000,"qty":1}]}' | jq -c .
sleep 4; echo "deals named web-1003: $(curl -s $SD/_orders | jq '[.[] | select(.externalId=="web-1003")] | length')"

step "4. Incoming call from a known client: the card goes to the responsible manager"
post "$API/webhooks/binotel?$TOK" -d '{"requestType":"receivedTheCall","callDetails":{"generalCallID":"5001","callType":"0","externalNumber":"0501234567","internalNumber":"101"}}' | jq -c .
grep 'msg=notify' out/crmbridge.log | tail -1

step "5. Missed calls: a lead is created; a repeat within 30 min only adds a note"
MISSED='{"requestType":"apiCallCompleted","callDetails":{"generalCallID":"%s","callType":"0","externalNumber":"0991234567","internalNumber":"101","disposition":"NOANSWER"}}'
post "$API/webhooks/binotel?$TOK" -d "$(printf "$MISSED" 5002)" | jq -c .
post "$API/webhooks/binotel?$TOK" -d "$(printf "$MISSED" 5002)" | jq -c .   # Binotel resends the same event
post "$API/webhooks/binotel?$TOK" -d "$(printf "$MISSED" 5003)" | jq -c .
sleep 4; crm | grep 380991234567 || crm | tail -2

step "6. A manager creates a TTN in the CRM; the bridge starts tracking it and moves the funnel"
ID=$(curl -s $SD/_orders | jq '.[] | select(.externalId=="web-1001") | .id')
post $SD/_ttn -d "{\"id\":$ID,\"ttn\":\"20451253019078\"}"
sleep 1
for code in 5 7 9; do
  post $NP/_set -d "{\"number\":\"20451253019078\",\"code\":$code}"
  sleep 5
  echo "Nova Poshta status $code -> deal: $(curl -s $SD/_orders | jq -c ".[] | select(.id==$ID) | {statusId}")"
done
echo "customer messages:"; grep 'msg=notify' out/crmbridge.log | grep 'to=telegram:42' | sed -E 's/.*text="?//'

step "7. A webhook without the secret is rejected"
curl -s -o /dev/null -w 'HTTP %{http_code}\n' -X POST $API/webhooks/binotel -d '{}'

printf '\nlogs: out/crmbridge.log\n'
