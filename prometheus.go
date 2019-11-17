/*
Copyright 2019 Kane York, Google LLC

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

// Package topk provides a Metric/Collector implementation of a top-K streaming
// summary algorithm for use with high cardinality data.
//
// The github.com/dgryski/go-topk package is used to implement the calculations.
package topk

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	tk "github.com/riking/go-prometheus-topk/internal/third_party/go-topk"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/model"
)

// TopK is a metric package for estimating the top keys of a high-cardinality set.
//
// On every collection, the top "K" label pairs (where K is the value of
// opts.Buckets) are exported as Counters with variable labels, plus a parallel
// set of Gauges for the error bars.
//
// Usage: call one of the With() methods to receive a TopKBucket, and call the
// Observe method to record an observation. If any NaN values are passed to
// Observe, they are treated as 0 so as to not pollute the storage.
type TopK interface {
	prometheus.Collector

	CurryWith(prometheus.Labels) (TopK, error)
	MustCurryWith(prometheus.Labels) TopK
	GetMetricWith(prometheus.Labels) (TopKBucket, error)
	GetMetricWithLabelValues(lvs ...string) (TopKBucket, error)
	With(prometheus.Labels) TopKBucket
	WithLabelValues(lvs ...string) TopKBucket
}

type TopKBucket interface {
	Observe(float64)
	Inc()
}

type TopKOpts struct {
	// Namespace, Subsystem, and Name are components of the fully-qualified
	// name of the TopK (created by joining these components with "_").
	// Only Name is mandatory, the others merely help structuring the name.
	// Note that the fully-qualified name of the TopK must be a valid
	// Prometheus metric name.
	Namespace string
	Subsystem string
	Name      string

	// Help provides information about this Histogram.
	//
	// Metrics with the same fully-qualified name must have the same Help
	// string.
	Help string

	// ConstLabels are used to attach fixed labels to this metric. Metrics
	// with the same fully-qualified name must have the same label names in
	// their ConstLabels.
	//
	// ConstLabels are only used rarely. In particular, do not use them to
	// attach the same labels to all your metrics. Those use cases are
	// better covered by target labels set by the scraping Prometheus
	// server, or by one specific metric (e.g. a build_info or a
	// machine_role metric). See also
	// https://prometheus.io/docs/instrumenting/writing_exporters/#target-labels,-not-static-scraped-labels
	ConstLabels prometheus.Labels

	// Buckets provides the number of metric streams that this metric is
	// expected to keep an accurate count for (the "K" in top-K).
	Buckets uint64
}

type topkRoot struct {
	streamMtx sync.Mutex
	stream    *tk.Stream

	countDesc *prometheus.Desc
	errDesc   *prometheus.Desc

	variableLabels []string
	writeMtx       sync.Mutex // Only used in Write method.
}

type curriedLabelValue struct {
	index int
	value string
}

type topkCurry struct {
	curry []curriedLabelValue
	root  *topkRoot
}

type topkWithLabelValues struct {
	compositeLabel string
	root           *topkRoot
}

type resolvedMetric struct {
	value      float64
	labelPairs []*dto.LabelPair
	ts         int64
}

var (
	_ TopK                = &topkCurry{}
	_ TopKBucket          = &topkWithLabelValues{}
	_ prometheus.Observer = &topkWithLabelValues{}
)

// NewTopK constructs a new TopK metric container.
func NewTopK(opts TopKOpts, labelNames []string) TopK {
	fqName := prometheus.BuildFQName(opts.Namespace, opts.Subsystem, opts.Name)

	// Take a copy to avoid shenanigans
	varLabels := append([]string(nil), labelNames...)

	root := &topkRoot{
		stream: tk.NewStream(int(opts.Buckets)),

		countDesc: prometheus.NewDesc(
			fqName, opts.Help, varLabels, opts.ConstLabels),
		errDesc: prometheus.NewDesc(
			fmt.Sprintf("%s_error", fqName), opts.Help, varLabels, opts.ConstLabels),

		variableLabels: varLabels,
	}
	return &topkCurry{root: root, curry: nil}
}

func (r *topkCurry) Describe(ch chan<- *prometheus.Desc) {
	ch <- r.root.countDesc
	ch <- r.root.errDesc
}

var labelParseSplit = string([]byte{model.SeparatorByte})

func (r *topkCurry) Collect(ch chan<- prometheus.Metric) {
	r.root.streamMtx.Lock()
	elts := r.root.stream.Keys()
	r.root.streamMtx.Unlock()

	for _, e := range elts {
		split := strings.Split(e.Key, labelParseSplit)
		if len(split) != len(r.root.variableLabels)+1 {
			panic("bad label-string value in topk")
		}
		lvs := split[:len(r.root.variableLabels)]
		ch <- prometheus.MustNewConstMetric(r.root.countDesc, prometheus.CounterValue, e.Count, lvs...)
		ch <- prometheus.MustNewConstMetric(r.root.errDesc, prometheus.GaugeValue, -e.Error, lvs...)
	}
}

func (b *topkWithLabelValues) Observe(v float64) {
	b.root.streamMtx.Lock()
	defer b.root.streamMtx.Unlock()
	b.root.stream.Insert(b.compositeLabel, v)
}

func (b *topkWithLabelValues) Inc() {
	b.Observe(1)
}

// note: label manipulation copied heavily from prometheus/client_golang/prometheus/vec.go

// MustCurryWith implements the Vec interface.
func (r *topkCurry) MustCurryWith(labels prometheus.Labels) TopK {
	n, err := r.CurryWith(labels)
	if err != nil {
		panic(err)
	}
	return n
}

// CurryWith implements the Vec interface.
func (r *topkCurry) CurryWith(labels prometheus.Labels) (TopK, error) {
	var (
		newCurry []curriedLabelValue
		oldCurry = r.curry
		iCurry   int
	)
	for i, label := range r.root.variableLabels {
		val, ok := labels[label]
		if iCurry < len(oldCurry) && oldCurry[iCurry].index == i {
			if ok {
				return nil, fmt.Errorf("label name %q is already curried", label)
			}
			newCurry = append(newCurry, oldCurry[iCurry])
			iCurry++
		} else {
			if !ok {
				continue // Label stays unset
			}
			newCurry = append(newCurry, curriedLabelValue{i, val})
		}
	}
	if leftover := len(oldCurry) + len(labels) - len(newCurry); leftover > 0 {
		return nil, fmt.Errorf("%d unknown label(s) found during currying", leftover)
	}

	return &topkCurry{
		curry: newCurry,
		root:  r.root,
	}, nil
}

func (r *topkCurry) compositeWithLabels(labels prometheus.Labels) (string, error) {
	if err := validateLabels(labels, len(r.root.variableLabels)-len(r.curry)); err != nil {
		return "", err
	}

	var (
		keyBuf bytes.Buffer
		curry  = r.curry
		iCurry int
	)

	for i, label := range r.root.variableLabels {
		val, ok := labels[label]
		if iCurry < len(curry) && curry[iCurry].index == i {
			if ok {
				return "", fmt.Errorf("label name %q is already curried", label)
			}
			keyBuf.WriteString(curry[iCurry].value)
			iCurry++
		} else {
			if !ok {
				return "", fmt.Errorf("label name %q missing in label map", label)
			}
			keyBuf.WriteString(val)
		}
		keyBuf.WriteByte(model.SeparatorByte)
	}
	return keyBuf.String(), nil
}

func (r *topkCurry) compositeWithLabelValues(lvs ...string) (string, error) {
	if err := validateLabelValues(lvs, len(r.root.variableLabels)-len(r.curry)); err != nil {
		return "", err
	}

	var (
		keyBuf        bytes.Buffer
		curry         = r.curry
		iVals, iCurry int
	)
	for i := 0; i < len(r.root.variableLabels); i++ {
		if iCurry < len(curry) && curry[iCurry].index == i {
			keyBuf.WriteString(curry[iCurry].value)
			iCurry++
		} else {
			keyBuf.WriteString(lvs[iVals])
			iVals++
		}
		keyBuf.WriteByte(model.SeparatorByte)
	}

	return keyBuf.String(), nil
}

func validateLabels(labels prometheus.Labels, expectCount int) error {
	if expectCount > len(labels) {
		return fmt.Errorf("not enough labels (missing %d)", expectCount-len(labels))
	} else if expectCount < len(labels) {
		return fmt.Errorf("received %d unrecognized labels", len(labels)-expectCount)
	}
	return nil
}

func validateLabelValues(lvs []string, expectCount int) error {
	if expectCount > len(lvs) {
		return fmt.Errorf("not enough labels (missing %d)", expectCount-len(lvs))
	} else if expectCount < len(lvs) {
		return fmt.Errorf("received %d unrecognized labels", len(lvs)-expectCount)
	}
	return nil
}

// GetMetricWith implements the Vec interface.
func (r *topkCurry) GetMetricWith(labels prometheus.Labels) (TopKBucket, error) {
	composite, err := r.compositeWithLabels(labels)
	if err != nil {
		return nil, err
	}
	return &topkWithLabelValues{compositeLabel: composite, root: r.root}, nil
}

// With implements the Vec interface.
func (r *topkCurry) With(labels prometheus.Labels) TopKBucket {
	composite, err := r.compositeWithLabels(labels)
	if err != nil {
		panic(err)
	}
	return &topkWithLabelValues{compositeLabel: composite, root: r.root}
}

// GetMetricWithLabelValues implements the Vec interface.
func (r *topkCurry) GetMetricWithLabelValues(lvs ...string) (TopKBucket, error) {
	composite, err := r.compositeWithLabelValues(lvs...)
	if err != nil {
		return nil, err
	}
	return &topkWithLabelValues{compositeLabel: composite, root: r.root}, nil
}

// WithLabelValues implements the Vec interface.
func (r *topkCurry) WithLabelValues(lvs ...string) TopKBucket {
	composite, err := r.compositeWithLabelValues(lvs...)
	if err != nil {
		panic(err)
	}
	return &topkWithLabelValues{compositeLabel: composite, root: r.root}
}
