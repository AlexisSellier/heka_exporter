package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/mozilla-services/heka/message"
	"github.com/prometheus/client_golang/prometheus"
)

type Config struct {
	Metrics []MetricConfig `json:"metrics"`
}

type MetricConfig struct {
	Name        string            `json:"name"`
	Help        string            `json:"help"`
	Type        string            `json:"type"`
	Value       string            `json:"value"`
	Matcher     string            `json:"matcher"`
	Labels      map[string]string `json:"labels"`
	ConstLabels map[string]string `json:"const_labels"`
	Buckets     []float64         `json:"buckets"`
	MatcherZero string            `json:"matcher_zero"`
}

func (m *MetricConfig) LabelKeysValues() ([]string, []string) {
	keys := make([]string, len(m.Labels))
	values := make([]string, len(m.Labels))
	i := 0
	for k, v := range m.Labels {
		keys[i] = k
		values[i] = v
		i++
	}
	return keys, values
}

type metric struct {
	counter      prometheus.Counter
	counterVec   *prometheus.CounterVec
	gauge        prometheus.Gauge
	gaugeVec     *prometheus.GaugeVec
	histogram    prometheus.Histogram
	histogramVec *prometheus.HistogramVec
	summary      prometheus.Summary
	summaryVec   *prometheus.SummaryVec
	matcher      *message.MatcherSpecification
	matcherZero  *message.MatcherSpecification
	LabelFields  []string
	MetricConfig
}

func (m *metric) Process(msg *message.Message) {
	field := m.MetricConfig.Value
	labels := extractLabels(m.LabelFields, msg)

	// If we don't need to initialize non-matching metrics, we can return early
	if m.matcherZero == nil && m.matcher != nil && !m.matcher.Match(msg) {
		return
	}
	// If we need to initialize non-match metrics, do only if matcher matches
	if m.matcherZero != nil && !m.matcherZero.Match(msg) {
		return
	}
	switch m.MetricConfig.Type {
	case "counter":
		if len(m.LabelFields) > 0 {
			m.counterVec.GetMetricWithLabelValues(labels...)
			if m.matcher != nil && !m.matcher.Match(msg) {
				break
			}

			if m.MetricConfig.Value == "" {
				m.counterVec.WithLabelValues(labels...).Inc()
			} else {
				value, err := getFieldFloatValue(field, msg)
				if err != nil {
					log.Println(err)
				}

				m.counterVec.WithLabelValues(labels...).Set(value)
			}

		} else {
			m.counter.Inc()
		}
	case "gauge":
		metric := m.gauge
		if len(m.LabelFields) > 0 {
			metric = m.gaugeVec.WithLabelValues(labels...)
		}
		if m.matcher != nil && !m.matcher.Match(msg) {
			break
		}
		value, err := getFieldFloatValue(field, msg)
		if err != nil {
			log.Println(err)
		}
		metric.Set(value)
	case "histogram":
		metric := m.histogram
		if len(m.LabelFields) > 0 {
			metric = m.histogramVec.WithLabelValues(labels...)
		}
		if m.matcher != nil && !m.matcher.Match(msg) {
			break
		}
		value, err := getFieldFloatValue(field, msg)
		if err != nil {
			log.Println(err)
		}
		metric.Observe(value)
	case "summary":
		metric := m.summary
		if len(m.LabelFields) > 0 {
			metric = m.summaryVec.WithLabelValues(labels...)
		}
		if m.matcher != nil && !m.matcher.Match(msg) {
			break
		}
		value, err := getFieldFloatValue(field, msg)
		if err != nil {
			log.Println(err)
		}
		metric.Observe(value)
	default:
	}
}

func getFieldValue(field string, msg *message.Message) (value interface{}) {
	var funcMap = map[string]func() string{
		"hostname":  msg.GetHostname,
		"logger":    msg.GetLogger,
		"pid":       func() string { return strconv.Itoa(int(msg.GetPid())) },
		"severity":  func() string { return strconv.Itoa(int(msg.GetSeverity())) },
		"timestamp": func() string { return strconv.Itoa(int(msg.GetTimestamp())) },
	}
	if field[0] == '@' {
		if f, ok := funcMap[field[1:]]; ok {
			return f()
		}
	}
	f, _ := msg.GetFieldValue(field)
	return f
}

func getFieldFloatValue(field string, msg *message.Message) (float64, error) {
	v := getFieldValue(field, msg)
	if v == nil {
		return 0.0, fmt.Errorf("Couldn't find field %s", field)
	}
	switch v := v.(type) {
	case float64:
		return v, nil
	case int:
		return float64(v), nil
	default:
		return 0.0, fmt.Errorf("Invalid type %T for field %s", v, field)
	}
}

func extractLabels(labels []string, msg *message.Message) []string {
	lvs := make([]string, len(labels))
	for i, l := range labels {
		v := getFieldValue(l, msg)
		lvs[i] = fmt.Sprintf("%v", v)
	}
	return lvs
}
func fieldToFloat(field *message.Field) (float64, error) {
	switch *field.ValueType {
	case message.Field_INTEGER:
		return float64(field.ValueInteger[0]), nil
	case message.Field_DOUBLE:
		return field.ValueDouble[0], nil
	case message.Field_BOOL:
		if field.ValueBool[0] {
			return 1.0, nil
		}
		return 0.0, fmt.Errorf("Can't convert %s to number", field.ValueType)
	default:
		return 0.0, fmt.Errorf("Value type %s not supported", *field.ValueType)
	}
}

type Bridge struct {
	metrics []metric
}

func newBridge(filename string) (*Bridge, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	config := &Config{}
	if err := json.Unmarshal(data, config); err != nil {
		return nil, err
	}
	bridge := &Bridge{}
	bridge.metrics = make([]metric, len(config.Metrics))
	for i, metric := range config.Metrics {
		if metric.Value == "" && metric.Type != "counter" {
			return nil, fmt.Errorf("Type %s requires a value field name", metric.Type)
		}
		bridge.metrics[i].MetricConfig = metric
		labels, fields := metric.LabelKeysValues()
		bridge.metrics[i].LabelFields = fields
		if len(labels) > 0 {
			switch metric.Type {
			case "counter":
				bridge.metrics[i].counterVec = prometheus.NewCounterVec(prometheus.CounterOpts{Name: metric.Name, Help: metric.Help, ConstLabels: metric.ConstLabels}, labels)
				prometheus.MustRegister(bridge.metrics[i].counterVec)
			case "gauge":
				bridge.metrics[i].gaugeVec = prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: metric.Name, Help: metric.Help, ConstLabels: metric.ConstLabels}, labels)
				prometheus.MustRegister(bridge.metrics[i].gaugeVec)
			case "histogram":
				bridge.metrics[i].histogramVec = prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: metric.Name, Help: metric.Help, Buckets: metric.Buckets, ConstLabels: metric.ConstLabels}, labels)
				prometheus.MustRegister(bridge.metrics[i].histogramVec)
			case "summary":
				bridge.metrics[i].summaryVec = prometheus.NewSummaryVec(prometheus.SummaryOpts{Name: metric.Name, Help: metric.Help, ConstLabels: metric.ConstLabels}, labels)
				prometheus.MustRegister(bridge.metrics[i].summaryVec)
			default:
				return nil, fmt.Errorf("Metric type %s is invalid", metric.Type)
			}
		} else {
			switch metric.Type {
			case "counter":
				bridge.metrics[i].counter = prometheus.NewCounter(prometheus.CounterOpts{Name: metric.Name, Help: metric.Help, ConstLabels: metric.ConstLabels})
				prometheus.MustRegister(bridge.metrics[i].counter)
			case "gauge":
				bridge.metrics[i].gauge = prometheus.NewGauge(prometheus.GaugeOpts{Name: metric.Name, Help: metric.Help, ConstLabels: metric.ConstLabels})
				prometheus.MustRegister(bridge.metrics[i].gauge)
			case "histogram":
				bridge.metrics[i].histogram = prometheus.NewHistogram(prometheus.HistogramOpts{Name: metric.Name, Help: metric.Help, Buckets: metric.Buckets, ConstLabels: metric.ConstLabels})
				prometheus.MustRegister(bridge.metrics[i].histogram)
			case "summary":
				bridge.metrics[i].summary = prometheus.NewSummary(prometheus.SummaryOpts{Name: metric.Name, Help: metric.Help, ConstLabels: metric.ConstLabels})
				prometheus.MustRegister(bridge.metrics[i].summary)
			default:
				return nil, fmt.Errorf("Metric type %s is invalid", metric.Type)
			}
		}
		for matcher, definition := range map[**message.MatcherSpecification]string{
			&bridge.metrics[i].matcher:     metric.Matcher,
			&bridge.metrics[i].matcherZero: metric.MatcherZero,
		} {
			if definition != "" {
				ms, err := message.CreateMatcherSpecification(definition)
				if err != nil {
					return nil, err
				}
				*matcher = ms
			}
		}
	}
	http.Handle("/metrics", prometheus.Handler())
	return bridge, nil
}

func (b *Bridge) Process(msg *message.Message) error {
	for _, metric := range b.metrics {
		metric.Process(msg)
	}
	return nil
}
