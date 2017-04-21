package es

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"

	log "github.com/Sirupsen/logrus"
	cricd "github.com/cricd/cricd-go"
	es "github.com/jetbasrawi/go.geteventstore"
	cache "github.com/patrickmn/go-cache"
)

var c = cache.New(5*time.Minute, 30*time.Second)

// TODO: Refactor how the config works. You're always going to use the default config, and if not you should be able to be override it

// CricdESClient defines the configuration required to connect to EventStore
type CricdESClient struct {
	client               *es.Client
	eventStoreURL        string
	eventStorePort       string
	eventStoreStreamName string
}

// UseDefaultConfig uses the configuration from ENV vars to determine the URL and port for EventStore
func (cricdClient *CricdESClient) UseDefaultConfig() {
	esURL := os.Getenv("EVENTSTORE_IP")
	if esURL != "" {
		cricdClient.eventStoreURL = esURL
	} else {
		log.WithFields(log.Fields{"value": "EVENTSTORE_IP"}).Info("Unable to find env var, using default `localhost`")
		cricdClient.eventStoreURL = "localhost"
	}

	esPort := os.Getenv("EVENTSTORE_PORT")
	if esPort != "" {
		cricdClient.eventStorePort = esPort
	} else {
		log.WithFields(log.Fields{"value": "EVENTSTORE_PORT"}).Info("Unable to find env var, using default `2113`")
		cricdClient.eventStorePort = "2113"
	}

	esStreamName := os.Getenv("EVENTSTORE_STREAM_NAME")
	if esStreamName != "" {
		cricdClient.eventStoreStreamName = esStreamName
	} else {
		log.WithFields(log.Fields{"value": "EVENTSTORE_STREAM_NAME"}).Info("Unable to find env var, using default `cricket_events_v1`")
		cricdClient.eventStoreStreamName = "cricket_events_v1"
	}

}

// Connect connects to EventStore. It returns a boolean value indicating the success of the connection
func (cricdClient *CricdESClient) Connect() bool {
	client, err := es.NewClient(nil, "http://"+cricdClient.eventStoreURL+":"+cricdClient.eventStorePort)
	if err != nil {
		log.WithFields(log.Fields{"value": err}).Fatal("Unable to create ES connection")
		return false
	}
	cricdClient.client = client
	return true
}

// PushEvent then pushes a cricd.Delivery to EventStore
// Returns the UUID of the event and an error if applicable
func (cricdClient *CricdESClient) PushEvent(event cricd.Delivery, dedupe bool) (string, error) {

	e, err := json.Marshal(event)
	if err != nil {
		// Handle errors
		log.WithFields(log.Fields{"value": err}).Error("Unable to marshal event to JSON")
		return "", err
	}

	// Store cache
	if dedupe == true {
		keySHA := sha256.Sum256([]byte(e))
		key := hex.EncodeToString(keySHA[:])
		_, found := c.Get(key)
		if found {
			log.WithFields(log.Fields{"value": key}).Error("Event already received in the last 5 minutes")
			return "", fmt.Errorf("Received this event in the last 5 minutes")
		}
		// In future we could just store nil here
		c.Set(key, &event, cache.DefaultExpiration)
	}
	uuid := es.NewUUID()
	myESEvent := es.NewEvent(uuid, "cricket_event", event, nil)

	// Create a new StreamWriter
	writer := cricdClient.client.NewStreamWriter(cricdClient.eventStoreStreamName)
	err = writer.Append(nil, myESEvent)
	if err != nil {
		// Handle errors
		log.WithFields(log.Fields{"value": err}).Error("Unable to push event to ES")
		return "", err
	}

	return uuid, nil

}

// ReadStream reads all events from a stream specified by streamName and returns them or an error if there was an issue reading from the stream
func (cricdClient *CricdESClient) ReadStream(streamName string) ([]interface{}, error) {
	reader := cricdClient.client.NewStreamReader(streamName)
	var allEvents []interface{}
	for reader.Next() {
		if reader.Err() != nil {
			switch err := reader.Err().(type) {

			case *url.Error, *es.ErrTemporarilyUnavailable:
				log.WithFields(log.Fields{"value": err}).Error("Server temporarily unavailable, retrying in 30s")
				<-time.After(time.Duration(30) * time.Second)

			case *es.ErrNotFound:
				log.WithFields(log.Fields{"value": err}).Error("Stream does not exist")
				return nil, errors.New("Unable to read from stream that does not exist")

			case *es.ErrUnauthorized:
				log.WithFields(log.Fields{"value": err}).Error("Unauthorized request")
				return nil, errors.New("Unauthorized to access ES")

			case *es.ErrNoMoreEvents:
				return allEvents, nil
			default:
				log.WithFields(log.Fields{"value": err}).Error("Unknown error occurred when reading from ES")
				return nil, errors.New("Unknown error occurred when reading from ES")
			}
		}
		var eventData interface{}
		var eventMeta interface{}
		err := reader.Scan(&eventData, &eventMeta)
		if err != nil {
			log.WithFields(log.Fields{"value": err}).Error("Unable to deserialize event")
			return nil, errors.New("Unable to deserialize event from ES")
		}
		allEvents = append(allEvents, eventData)
	}
	return allEvents, nil
}
