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

const metricName = "test_metric"

func TestRegisterCollect(t *testing.T) {
	const numBuckets = 3

	reg := prometheus.NewPedanticRegistry()
	k := NewTopK(TopKOpts{
		Name:    metricName,
		Buckets: numBuckets,
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

	for _, v := range mets {
		if *v.Name == metricName {
			if len(v.Metric) != numBuckets {
				t.Errorf("wrong metric count: got %v expected %v", len(mets), numBuckets)
			}
		}
	}
}
