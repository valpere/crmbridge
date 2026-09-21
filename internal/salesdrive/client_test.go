package salesdrive_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/valpere/crmbridge/internal/fakesalesdrive"
	"github.com/valpere/crmbridge/internal/salesdrive"
)

func setup(t *testing.T) (*salesdrive.Client, *fakesalesdrive.Server) {
	t.Helper()
	fake := fakesalesdrive.New("key")
	ts := httptest.NewServer(fake.Handler())
	t.Cleanup(ts.Close)
	return salesdrive.New(salesdrive.Config{BaseURL: ts.URL, APIKey: "key"}), fake
}

func TestOrderLifecycle(t *testing.T) {
	c, fake := setup(t)
	ctx := context.Background()
	id, err := c.CreateOrder(ctx, salesdrive.NewOrder{ExternalID: "e1", Phone: "380501234567", FName: "Іра", LName: "Коваль",
		Products: []salesdrive.Product{{Name: "Кава", CostPerItem: 125, Amount: 2}}})
	if err != nil || id == 0 {
		t.Fatalf("create: %d %v", id, err)
	}
	if got, ok, err := c.FindByExternalID(ctx, "e1"); err != nil || !ok || got != id {
		t.Fatalf("find: %d %v %v", got, ok, err)
	}
	if _, ok, _ := c.FindByExternalID(ctx, "nope"); ok {
		t.Fatal("unknown external id must not be found")
	}
	if err := c.UpdateStatus(ctx, salesdrive.Ref{ExternalID: "e1"}, 21); err != nil {
		t.Fatal(err)
	}
	if err := c.UpdateStatus(ctx, salesdrive.Ref{ID: id}, 22); err != nil {
		t.Fatal(err)
	}
	if err := c.AddNote(ctx, id, "Пропущений дзвінок"); err != nil {
		t.Fatal(err)
	}
	o := fake.Orders()[0]
	if o.StatusID != 22 || len(o.Notes) != 1 {
		t.Fatalf("state: %+v", o)
	}
	if err := c.UpdateStatus(ctx, salesdrive.Ref{ID: 999}, 1); err == nil || salesdrive.Retryable(err) {
		t.Fatalf("unknown order must be a permanent error: %v", err)
	}
}

func TestManagerByPhone(t *testing.T) {
	c, _ := setup(t)
	ctx := context.Background()
	if ct, err := c.ManagerByPhone(ctx, "0501234567"); err != nil || ct != nil {
		t.Fatalf("unknown caller: %v %v", ct, err)
	}
	c.CreateOrder(ctx, salesdrive.NewOrder{Phone: "+380 50 123 45 67", FName: "Іра", LName: "Коваль"})
	ct, err := c.ManagerByPhone(ctx, "0501234567")
	if err != nil || ct == nil || ct.ClientName != "Іра Коваль" || ct.ManagerName == "" || !ct.KnownClient {
		t.Fatalf("known caller: %+v %v", ct, err)
	}
}

func TestAuthAndRetryClassification(t *testing.T) {
	_, fake := setup(t)
	ts := httptest.NewServer(fake.Handler())
	defer ts.Close()
	bad := salesdrive.New(salesdrive.Config{BaseURL: ts.URL, APIKey: "wrong"})
	if _, err := bad.CreateOrder(context.Background(), salesdrive.NewOrder{Phone: "1"}); err == nil || salesdrive.Retryable(err) {
		t.Fatalf("wrong key must be permanent: %v", err)
	}
	good := salesdrive.New(salesdrive.Config{BaseURL: ts.URL, APIKey: "key"})
	fake.FailNext(1)
	if _, err := good.CreateOrder(context.Background(), salesdrive.NewOrder{Phone: "1"}); err == nil || !salesdrive.Retryable(err) {
		t.Fatalf("503 must be retryable: %v", err)
	}
	if _, err := good.CreateOrder(context.Background(), salesdrive.NewOrder{}); err == nil || salesdrive.Retryable(err) {
		t.Fatalf("validation failure must be permanent: %v", err)
	}
}
