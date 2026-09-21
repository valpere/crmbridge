package novaposhta_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/valpere/crmbridge/internal/fakenp"
	"github.com/valpere/crmbridge/internal/novaposhta"
)

func TestTrack(t *testing.T) {
	fake := fakenp.New()
	ts := httptest.NewServer(fake.Handler())
	defer ts.Close()
	c := novaposhta.New(novaposhta.Config{BaseURL: ts.URL, APIKey: "k"})
	fake.Set("59000000000001", 7)
	fake.Set("59000000000002", 9)
	got, err := c.Track(context.Background(), []string{"59000000000001", "59000000000002", "59000000000003"})
	if err != nil {
		t.Fatal(err)
	}
	if got["59000000000001"].Code != 7 || got["59000000000002"].Code != 9 || got["59000000000003"].Code != 3 {
		t.Fatalf("%+v", got)
	}
	if _, err := c.Track(context.Background(), make([]string, 101)); err == nil {
		t.Fatal("more than 100 documents must be refused client-side")
	}
	fake.FailNext(1)
	if _, err := c.Track(context.Background(), []string{"1"}); err == nil {
		t.Fatal("HTTP 503 must surface as an error")
	}
	if m, err := c.Track(context.Background(), nil); err != nil || len(m) != 0 {
		t.Fatal("empty input is a no-op")
	}
}

func TestClassify(t *testing.T) {
	type c struct {
		code  int
		stage novaposhta.Stage
		final bool
	}
	for _, x := range []c{
		{1, novaposhta.StageCreated, false}, {5, novaposhta.StageTransit, false}, {101, novaposhta.StageTransit, false},
		{7, novaposhta.StageArrived, false}, {8, novaposhta.StageArrived, false},
		{9, novaposhta.StageReceived, true}, {11, novaposhta.StageReceived, true},
		{102, novaposhta.StageRefused, true}, {108, novaposhta.StageRefused, true},
		{2, novaposhta.StageDeleted, true}, {3, novaposhta.StageDeleted, false}, {999, novaposhta.StageUnknown, false},
	} {
		st, _ := novaposhta.Classify(x.code)
		if st != x.stage || novaposhta.Final(x.code) != x.final {
			t.Errorf("code %d: stage=%s final=%v", x.code, st, novaposhta.Final(x.code))
		}
	}
	_, r5 := novaposhta.Classify(5)
	_, r7 := novaposhta.Classify(7)
	_, r9 := novaposhta.Classify(9)
	if !(r5 < r7 && r7 < r9) {
		t.Fatal("rank must grow along the delivery path")
	}
}
