# heka_exporter
The heka_exporter reads a [config file](metrics.sample.json) which describes a
set of [Prometheus](http://prometheus.io) metrics. It listens for heka
messages, extracts the specified fields and exposes the metrics for consumption
by Prometheus.

## Building it
The exporter depends on the heka go libraries. You need to *build* them first
or at least use yacc to create the parser code:

    cd $GOPATH/src/github.com/mozilla-services/heka/message
    go tool yacc -l=false -o=message_matcher_parser.go message_matcher_parser.y

Then you can run `go build` in the root of this repository to build the
heka_exporter.
