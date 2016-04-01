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
	Buckets     []float64         `json:buckets`
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
	LabelFields  []string
	MetricConfig
}

func (m *metric) Process(msg *message.Message) {
	if m.matcher != nil && !m.matcher.Match(msg) {
		return
	}
	value := 0.0
	if m.MetricConfig.Value != "" {
		v := getFieldValue(m.MetricConfig.Value, msg)
		if v == nil {
			log.Println("Couldn't find field", m.MetricConfig.Value)
			return
		}
		switch v := v.(type) {
		case float64:
			value = v
		case int:
			value = float64(v)
		default:
			log.Printf("Invalid type %T for field %s", v, m.MetricConfig.Value)
			return
		}
	}

	switch m.MetricConfig.Type {
	case "counter":
		if len(m.LabelFields) > 0 {
			if m.MetricConfig.Value == "" {
				m.counterVec.WithLabelValues(extractLabels(m.LabelFields, msg)...).Inc()
			} else {
				m.counterVec.WithLabelValues(extractLabels(m.LabelFields, msg)...).Set(value)
			}
		} else {
			m.counter.Inc()
		}
	case "gauge":
		if len(m.LabelFields) > 0 {
			m.gaugeVec.WithLabelValues(extractLabels(m.LabelFields, msg)...).Set(value)
		} else {
			m.gauge.Set(value)
		}
	case "histogram":
		if len(m.LabelFields) > 0 {
			m.histogramVec.WithLabelValues(extractLabels(m.LabelFields, msg)...).Observe(value)
		} else {
			m.histogram.Observe(value)
		}
	case "summary":
		if len(m.LabelFields) > 0 {
			m.summaryVec.WithLabelValues(extractLabels(m.LabelFields, msg)...).Observe(value)
		} else {
			m.summary.Observe(value)
		}
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

		if metric.Matcher != "" {
			matcher, err := message.CreateMatcherSpecification(metric.Matcher)
			if err != nil {
				return nil, err
			}
			bridge.metrics[i].matcher = matcher
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
