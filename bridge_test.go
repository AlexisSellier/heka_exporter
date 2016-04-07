package main

import (
	"testing"

	"github.com/mozilla-services/heka/message"
)

func newField(name string, value interface{}) *message.Field {
	field, err := message.NewField(name, value, "")
	if err != nil {
		panic(err)
	}
	return field
}

func newTestMessage() *message.Message {
	msg := &message.Message{}
	msg.SetHostname("srv001")
	msg.SetLogger("foo")
	for k, v := range map[string]interface{}{
		"status": 500,
		"float":  1.6,
		"up":     true,
	} {
		msg.AddField(newField(k, v))
	}
	return msg
}

func TestGetFieldValue(t *testing.T) {
	msg := newTestMessage()
	if expect, got := "srv001", getFieldValue("@hostname", msg); expect != got {
		t.Fatalf("Expect: %s, Got: %s", expect, got)
	}
	if expect, got := "foo", getFieldValue("@logger", msg); expect != got {
		t.Fatalf("Expect: %s, Got: %s", expect, got)
	}
	if expect, got := int64(500), getFieldValue("status", msg); expect != got {
		t.Fatalf("Expect: %s, Got: %s", expect, got)
	}
	if expect, got := 1.6, getFieldValue("float", msg); expect != got {
		t.Fatalf("Expect: %s, Got: %s", expect, got)
	}
	if expect, got := true, getFieldValue("up", msg); expect != got {
		t.Fatalf("Expect: %s, Got: %s", expect, got)
	}
}

var fieldToFloatTests = map[*message.Field]float64{
	newField("", 23.5): 23.5,
	newField("", 5):    5.0,
	newField("", true): 1.0,
}

func TestFieldToFloat(t *testing.T) {
	for field, expect := range fieldToFloatTests {
		got, err := fieldToFloat(field)
		if err != nil {
			t.Fatal("Expected %s but got error: %s", expect, err)
		}
		if expect != got {
			t.Fatal("Expect: %s, Got: %s", expect, got)
		}
	}
}

func TestNewBridge(t *testing.T) {
	bridge, err := newBridge("metrics.sample.json")
	if err != nil {
		t.Fatal(err)
	}
	if expect, got := 5, len(bridge.metrics); expect != got {
		t.Fatalf("Expect: %s, Got: %s", expect, got)
	}
	if bridge.metrics[2].gaugeVec == nil {
		t.Fatal("Expected 3rd metric to be gaugeVec")
	}
	if expect, got := "Logger == 'x'", bridge.metrics[2].MetricConfig.Matcher; expect != got {
		t.Fatalf("Expect: %s, Got: %s", expect, got)
	}
	if bridge.metrics[2].matcher == nil {
		t.Fatal("Expected matcher to be setup")
	}
}
