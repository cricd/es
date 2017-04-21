package es

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	cricd "github.com/cricd/cricd-go"
)

// type validateJSONTest struct {
// 	fileName string
// 	valid    bool
// }

// var validateJSONTests = []validateJSONTest{
// 	{"test/good_event.json", true},
// 	{"test/bad_event.json", false},
// }

// func TestValidateJSON(t *testing.T) {
// 	for _, test := range validateJSONTests {
// 		expected := test.valid
// 		s, err := ioutil.ReadFile(test.fileName)
// 		if err != nil {
// 			panic(err)
// 		}
// 		actual := validateJSON(string(s))
// 		if actual != expected {
// 			t.Errorf("Failed to validate json for %v expected %v got %v", test.fileName, expected, actual)
// 		}
// 	}

// }

type useDefaultConfigTest struct {
	input  CricdESClient
	output CricdESClient
}

var useDefaultConfigTests = []useDefaultConfigTest{
	{CricdESClient{nil, "google.com", "1337", "foo"}, CricdESClient{nil, "google.com", "1337", "foo"}},
	{CricdESClient{nil, "", "", ""}, CricdESClient{nil, "localhost", "2113", "cricket_events_v1"}},
}

func TestUseDefaultConfig(t *testing.T) {
	for _, test := range useDefaultConfigTests {
		inConfig := test.input
		os.Setenv("EVENTSTORE_IP", inConfig.eventStoreURL)
		os.Setenv("EVENTSTORE_PORT", inConfig.eventStorePort)
		os.Setenv("EVENTSTORE_STREAM_NAME", inConfig.eventStoreStreamName)
		expected := test.output
		inConfig.UseDefaultConfig()
		if inConfig != expected {
			t.Errorf("Failed to get config expected %v but got %v", expected, inConfig)
		}
	}
}

// type connectTest struct {
// 	client    CricdESClient
// 	connected bool
// }

// var connectTests = []connectTest{
// 	{CricdESClient{nil, "localhost", "2113", "cricket_events_v1"}, true},
// }

// func TestConnect(t *testing.T) {
// 	for _, test := range connectTests {
// 		os.Setenv("EVENTSTORE_IP", test.client.eventStoreURL)
// 		os.Setenv("EVENTSTORE_PORT", test.client.eventStorePort)
// 		os.Setenv("EVENTSTORE_STREAM_NAME", test.client.eventStoreStreamName)
// 		test.client.UseDefaultConfig()
// 		expected := test.connected
// 		actual := test.client.Connect()
// 		if expected != actual {
// 			t.Errorf("Failed to connect to ES expected %v but got %v", expected, actual)
// 		}
// 	}
// }

type pushEventTest struct {
	eventFile string
	valid     bool
}

var pushEventTests = []pushEventTest{
	{"test/good_event.json", true},
	{"test/bad_event.json", false},
}

func TestPushEvent(t *testing.T) {
	var testClient CricdESClient
	testClient.UseDefaultConfig()
	testClient.Connect()
	for _, test := range pushEventTests {
		s, err := ioutil.ReadFile(test.eventFile)
		if err != nil {
			panic(err)
		}
		var d cricd.Delivery
		_ = json.Unmarshal(s, &d)

		expected := test.valid
		uuid, _ := testClient.PushEvent(d, false)
		actual := (uuid != "")
		if actual != expected {
			t.Errorf("Failed to push to ES expected %v but got %v", expected, actual)
		}
	}
}
