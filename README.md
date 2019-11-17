# go-prometheus-topk

A custom Prometheus metric exporter wrapping the github.com/dgryski/go-topk library.

By using top-K metric collection, you can estimate the outliers of
high-cardinality data without storing the entire dataset. (If you need
something more precise than "estimates", then structured log processing is the
way to go.)

## Status

This is not an officially supported Google product.
