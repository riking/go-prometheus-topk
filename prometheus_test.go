package topk

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisterCollect(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	k := NewTopK(TopKOpts{
		Name:    "test_metric",
		Buckets: 3,
	}, []string{"key"})

	// Verify that registration and gathering work
	err := reg.Register(k)
	if err != nil {
		t.Fatal(err)
	}

	_, err = reg.Gather()
	if err != nil {
		t.Error(err)
	}

	// Write some data
	k.WithLabelValues("a").Inc()
	k.WithLabelValues("b").Inc()
	k.WithLabelValues("c").Inc()
	k.WithLabelValues("d").Observe(1.5)
	k.WithLabelValues("a").Inc()

	// Verify collection still works
	mets, err := reg.Gather()
	if err != nil {
		t.Error(err)
	}

	return

	for _, v := range mets {
		t.Logf("# %s %v %s", *v.Name, v.Type, *v.Help)
		for _, m := range v.Metric {
			t.Logf("%v", m)
		}
	}
}
