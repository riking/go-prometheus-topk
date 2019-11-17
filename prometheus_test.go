/*
Copyright 2019 Google LLC
Copyright 2019 Kane York

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
