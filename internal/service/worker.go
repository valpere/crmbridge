package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/valpere/crmbridge/internal/novaposhta"
	"github.com/valpere/crmbridge/internal/salesdrive"
	"github.com/valpere/crmbridge/internal/store"
)

// Run drives the outbox and the parcel tracker until ctx is done.
func (s *Service) Run(ctx context.Context, tick time.Duration) {
	t := time.NewTicker(tick)
	defer t.Stop()
	for {
		if _, err := s.ProcessOnce(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("outbox", "err", err)
		}
		if _, err := s.PollTTNs(ctx); err != nil && ctx.Err() == nil {
			s.log.Warn("tracking poll failed, will retry next round", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ProcessOnce works through the outbox jobs that are due, oldest first.
func (s *Service) ProcessOnce(ctx context.Context) (int, error) {
	var jobs []store.Job
	if err := s.st.Do(ctx, func(tx *store.Tx) (err error) {
		jobs, err = tx.DueJobs(ctx, s.now(), 50)
		return
	}); err != nil {
		return 0, err
	}
	for _, j := range jobs {
		if err := s.step(ctx, j); err != nil {
			return 0, err
		}
	}
	return len(jobs), nil
}

func (s *Service) finish(ctx context.Context, j store.Job, then func(*store.Tx) error) error {
	j.State, j.LastError = store.JobDone, ""
	return s.st.Do(ctx, func(tx *store.Tx) error {
		if err := tx.UpdateJob(ctx, j); err != nil {
			return err
		}
		if then != nil {
			return then(tx)
		}
		return nil
	})
}

func (s *Service) fail(ctx context.Context, j store.Job, why string) error {
	s.log.Error("CRM job failed", "job", j.ID, "key", j.DedupeKey, "reason", why)
	j.State, j.LastError = store.JobFailed, why
	return s.st.Do(ctx, func(tx *store.Tx) error { return tx.UpdateJob(ctx, j) })
}

// failed handles an error from the CRM: transient ones are retried with
// exponential backoff, permanent ones fail the job at once.
func (s *Service) failed(ctx context.Context, j store.Job, err error) error {
	if !salesdrive.Retryable(err) {
		return s.fail(ctx, j, err.Error())
	}
	j.Attempts++
	j.LastError = err.Error()
	if j.Attempts >= s.cfg.MaxAttempts {
		return s.fail(ctx, j, fmt.Sprintf("gave up after %d attempts: %v", j.Attempts, err))
	}
	d := s.cfg.BackoffBase << (j.Attempts - 1)
	if d > s.cfg.BackoffMax || d <= 0 {
		d = s.cfg.BackoffMax
	}
	j.NextAt = s.now().Add(d)
	s.log.Warn("CRM job will be retried", "job", j.ID, "key", j.DedupeKey, "attempt", j.Attempts, "in", d, "err", err)
	return s.st.Do(ctx, func(tx *store.Tx) error { return tx.UpdateJob(ctx, j) })
}

func (s *Service) step(ctx context.Context, j store.Job) error {
	switch j.Kind {
	case store.KindCreateOrder:
		var p createPayload
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return s.fail(ctx, j, "corrupt payload: "+err.Error())
		}
		var id int64
		var found bool
		var err error
		if j.Attempts > 0 {
			// An earlier attempt may have created the order even though we saw
			// an error (lost answer): look before creating a second one.
			if id, found, err = s.crm.FindByExternalID(ctx, p.ExternalID); err != nil {
				return s.failed(ctx, j, err)
			}
		}
		if !found {
			if id, err = s.crm.CreateOrder(ctx, p.Order); err != nil {
				return s.failed(ctx, j, err)
			}
		}
		return s.finish(ctx, j, func(tx *store.Tx) error { return tx.SetSDOrderID(ctx, p.ExternalID, id) })

	case store.KindNote:
		var p notePayload
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return s.fail(ctx, j, "corrupt payload: "+err.Error())
		}
		id, wait, err := s.sdID(ctx, p.ExternalID)
		if err != nil {
			return err
		}
		if wait == "failed" {
			return s.fail(ctx, j, "the order this note belongs to was never created")
		}
		if id == 0 { // its creation is still in the queue: try again shortly
			j.NextAt = s.now().Add(2 * time.Second)
			return s.st.Do(ctx, func(tx *store.Tx) error { return tx.UpdateJob(ctx, j) })
		}
		if err := s.crm.AddNote(ctx, id, p.Text); err != nil {
			return s.failed(ctx, j, err)
		}
		return s.finish(ctx, j, nil)

	case store.KindStatus:
		var p statusPayload
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return s.fail(ctx, j, "corrupt payload: "+err.Error())
		}
		if p.SDOrderID == 0 && p.ExternalID != "" {
			id, _, err := s.sdID(ctx, p.ExternalID)
			if err != nil {
				return err
			}
			p.SDOrderID = id
		}
		if p.SDOrderID == 0 && p.ExternalID == "" {
			return s.fail(ctx, j, "no CRM order to update")
		}
		if err := s.crm.UpdateStatus(ctx, salesdrive.Ref{ID: p.SDOrderID, ExternalID: p.ExternalID}, p.StatusID); err != nil {
			return s.failed(ctx, j, err)
		}
		return s.finish(ctx, j, nil)
	}
	return s.fail(ctx, j, "unknown job kind "+j.Kind)
}

// sdID returns the CRM id of an order we created; wait is "failed" when its
// creation job has failed for good.
func (s *Service) sdID(ctx context.Context, ext string) (id int64, wait string, err error) {
	err = s.st.Do(ctx, func(tx *store.Tx) error {
		o, e := tx.GetOrder(ctx, ext)
		if e != nil {
			return e
		}
		id = o.SDOrderID
		if id == 0 {
			if cj, e := tx.JobByKey(ctx, "create:"+ext); e == nil && cj.State == store.JobFailed {
				wait = "failed"
			}
		}
		return nil
	})
	return
}

// Requeue retries a failed job once its cause is fixed.
func (s *Service) Requeue(ctx context.Context, id int64) error {
	return s.st.Do(ctx, func(tx *store.Tx) error {
		ok, err := tx.Requeue(ctx, id, s.now())
		if err != nil {
			return err
		}
		if !ok {
			return store.ErrNotFound
		}
		return nil
	})
}

// ---- parcel tracking ---------------------------------------------------------

type notice struct{ to, text string }

// PollTTNs asks Nova Poshta about every unfinished parcel that is due (in
// batches of at most 100) and turns forward progress into a CRM status
// change and a customer message. A status that moves backwards, or a repeat,
// is ignored, so flapping answers cannot drag an order back in the funnel.
func (s *Service) PollTTNs(ctx context.Context) (int, error) {
	var due []store.TTN
	if err := s.st.Do(ctx, func(tx *store.Tx) (err error) {
		due, err = tx.DueTTNs(ctx, s.now().Add(-s.cfg.PollEvery), novaposhta.BatchSize)
		return
	}); err != nil || len(due) == 0 {
		return 0, err
	}
	nums := make([]string, len(due))
	for i, x := range due {
		nums[i] = x.Number
	}
	got, err := s.np.Track(ctx, nums)
	if err != nil {
		return 0, err
	}
	var notes []notice
	err = s.st.Do(ctx, func(tx *store.Tx) error {
		for _, x := range due {
			x.PolledAt = s.now()
			if s.now().Sub(x.AddedAt) > s.cfg.MaxTrackAge {
				x.Final = true
			}
			if tr, ok := got[x.Number]; ok {
				stage, rank := novaposhta.Classify(tr.Code)
				if rank > x.LastRank && stage != novaposhta.StageUnknown {
					prev := x.LastRank
					x.LastCode, x.LastRank = tr.Code, rank
					x.Final = x.Final || novaposhta.Final(tr.Code)
					if sid, ok := s.cfg.StatusMap[tr.Code]; ok {
						if err := s.enqueue(ctx, tx, fmt.Sprintf("status:%s:%d", x.Number, tr.Code), store.KindStatus,
							statusPayload{SDOrderID: x.SDOrderID, ExternalID: x.ExternalID, StatusID: sid}); err != nil {
							return err
						}
					}
					n, err := s.customerNotice(ctx, tx, x, stage, prev)
					if err != nil {
						return err
					}
					notes = append(notes, n...)
				}
			}
			if err := tx.UpdateTTN(ctx, x); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	for _, n := range notes {
		if err := s.n.Send(ctx, n.to, n.text); err != nil {
			s.log.Warn("could not send status message", "to", n.to, "err", err)
		}
	}
	return len(due), nil
}

func (s *Service) customerNotice(ctx context.Context, tx *store.Tx, x store.TTN, stage novaposhta.Stage, prevRank int) ([]notice, error) {
	to := ""
	label := "ТТН " + x.Number
	if x.ExternalID != "" {
		label = "Замовлення " + x.ExternalID + " (ТТН " + x.Number + ")"
		if o, err := tx.GetOrder(ctx, x.ExternalID); err == nil {
			to = o.Contact
		}
	}
	switch stage {
	case novaposhta.StageTransit:
		if prevRank < 2 {
			return []notice{{to, label + ": відправлено, посилка в дорозі."}}, nil
		}
	case novaposhta.StageArrived:
		return []notice{{to, label + ": посилка прибула на відділення Нової Пошти. Забирайте!"}}, nil
	case novaposhta.StageReceived:
		return []notice{{to, label + ": отримано. Дякуємо за замовлення!"}}, nil
	case novaposhta.StageRefused:
		return []notice{{s.cfg.DefaultManager, label + ": відмова або повернення посилки. Потрібна реакція менеджера."}}, nil
	}
	return nil, nil
}
