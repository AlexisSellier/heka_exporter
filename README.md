# heka_exporter
The heka_exporter reads a [config file](metrics.sample.json) which describes a
set of [Prometheus](http://prometheus.io) metrics. It listens for
[heka](http://hekad.readthedocs.org/) messages, extracts the specified fields
and exposes the metrics for consumption by Prometheus.

## Configuration
The heka_exporter is configured by providing a json file with a list of metric
definitions under the key `metrics`:

```
{
  "metrics": [
    { ... },
    { ... }
  ]
}
```

Each metric definition consists of:

- name: The name of the resulting Prometheus metric
- help: The help string for the Prometheus metric
- type: The metric type (counter, gauge, histogram or summary)
- labels: Dynamatic labels where value refers to Heka variables
- matcher: (Optional, all matched by default) Only messages matching this Heka
  matcher will be considered
- value: (Optional for counters) The Heka variables to take the metric value from
- buckets: (Required for histograms) Buckets into which observations are
  counted
- const_labels: (Optional) set of static labels for metric

`value` can be omitted for counters in which case the metric get incremented by 1
for every message that matched. If a matcher is set, only the matching messages
will get processed. For details on the matcher syntax, see the
[Heka documentation](http://hekad.readthedocs.org/en/latest/message_matcher.html).

The values in `labels` and `value` refer to Heka fields. To access other
variables, the following @-prefixed keywords can be used:

- @hostname
- @logger
- @pid
- @severity
- @timestamp

## Building it
The exporter depends on the heka go libraries. You need to *build* them first
or at least use yacc to create the parser code:

    cd $GOPATH/src/github.com/mozilla-services/heka/message
    go tool yacc -l=false -o=message_matcher_parser.go message_matcher_parser.y

Run `go build` in the root of this repository to build the
heka_exporter.
