package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/airware/kube2logstash/logstash"
	"github.com/kr/pretty"
	flag "github.com/spf13/pflag"
	"k8s.io/kubernetes/pkg/api"
	client "k8s.io/kubernetes/pkg/client/clientset_generated/release_1_2"
	"k8s.io/kubernetes/pkg/client/restclient"
	"k8s.io/kubernetes/pkg/fields"
	"k8s.io/kubernetes/pkg/labels"
)

const logstashMessageType = "kubernetes"

var shutdown = make(chan struct{})

var (
	flagKubernetesURL string
	flagLogstashHost  string
	flagLogstashPort  int
	flagDebug         bool
)

func init() {
	flag.StringVar(&flagKubernetesURL, "kubernetes-url", "", "kubernetes url")
	flag.StringVar(&flagLogstashHost, "logstash-host", "logstash", "logstash hostname or ip")
	flag.IntVar(&flagLogstashPort, "logstash-port", 5859, "TCP Port for logstash")
	flag.BoolVar(&flagDebug, "debug", false, "Enable debug logging")
}

func main() {
	flag.Parse()

	if flagDebug {
		log.SetLevel(log.DebugLevel)
	}

	// Setup K8S
	kc, err := setupKubernetesClient()
	if err != nil {
		log.WithError(err).Fatal("failed to setup kubernetes client")
	}
	log.Infof("Established kubernetes connection")

	// Setup Logstash
	lc, err := logstash.Connect(flagLogstashHost, flagLogstashPort)
	if err != nil {
		log.WithError(err).Fatal("failed to connect to logstash")
	}
	log.Infof("Established logstash connection")

	var messageCounter uint32
	go func() {
		t := time.NewTicker(time.Second * 60)
		for range t.C {
			i := atomic.LoadUint32(&messageCounter)
			if i == 0 {
				continue
			}
			log.Infof("Sent %d messages in the last 15s", i)
			atomic.StoreUint32(&messageCounter, 0)
		}
	}()

	wg := &sync.WaitGroup{}
	messages := make(chan *LogstashMessage)
	// Start k8s watcher loop
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-shutdown:
				log.Info("watcher loop shut down")
				return
			default:
			}
			// setupWatcher will return nil if the connection is closed thus restarting the loop and watcher
			log.Debug("Starting watcher")
			err := setupWatcher(kc, messages)
			if err != nil {
				log.WithError(err).Errorf("watcher failed")
				close(messages)
				return
			}
		}
	}()

	// Start watching messages channel and sending to logstash
	log.Debug("Waiting for messages")
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-shutdown:
				log.Infof("Shutdown message loop")
				return
			case msg, ok := <-messages:
				if !ok {
					log.Warnf("messages channel closed")
					return
				}
				atomic.AddUint32(&messageCounter, 1)
				lsmJSON, err := json.Marshal(msg)
				if err != nil {
					log.WithError(err).Warnf("failed to convert logstashMessage to JSON")
					continue
				}
				lsmJSON = append(lsmJSON, '\n')
				// Send messages to logstash
				err = lc.Send(lsmJSON)
				if err != nil {
					log.WithError(err).Errorf("send failed")
				}
			}
		}
	}()

	// Wait for shutdown
	go func() {
		sigChan := make(chan os.Signal, 2)
		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
		select {
		case signal := <-sigChan:
			close(shutdown)
			log.Infof("Exiting on %v", signal)
		}
	}()
	wg.Wait()
	log.Infof("All workers shut down")
}

func setupKubernetesClient() (*client.Clientset, error) {
	var cfg *restclient.Config
	var err error
	if len(flagKubernetesURL) > 0 {
		cfg = &restclient.Config{
			Host: flagKubernetesURL,
		}
	} else {
		cfg, err = restclient.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get cluster config")
		}
	}
	return client.NewForConfig(cfg)
}

// setupWatcher will establish a k8s event watch and send messages to the provided channel
// it blocks until the watch expires (the ResultChan closes)
// a nil error means nothing went wrong, the connection just timed out and thus it's safe to retry
func setupWatcher(kc client.Interface, messages chan *LogstashMessage) error {
	watcher, err := kc.Core().Events(api.NamespaceAll).Watch(api.ListOptions{
		LabelSelector: labels.Everything(),
		FieldSelector: fields.Everything(),
		Watch:         true,
	})
	if err != nil {
		return fmt.Errorf("failed to start event watcher: %s", err)
	}

	go func() {
		<-shutdown
		watcher.Stop()
	}()

	for evt := range watcher.ResultChan() {
		var logstashMessage *LogstashMessage
		switch typedEvt := evt.Object.(type) {
		default:
			log.Warn("Unknown event type")
			pretty.Print(typedEvt)
		case *api.Event:
			logstashMessage, err = eventToLogstash(typedEvt)
			if err != nil {
				log.WithError(err).Warnf("failed to process message")
				continue
			}
			log.Debug("Sending Message")
			messages <- logstashMessage
		}

	}
	log.Debug("ResultChan was closed")
	return nil
}

// LogstashMessage represents the format sent to logstash for processing
type LogstashMessage struct {
	Name           string    `json:"name"`
	Namespace      string    `json:"namespace"`
	PodName        string    `json:"pod_name,omitempty"`
	Kind           string    `json:"kind"`
	Reason         string    `json:"reason"`
	Message        string    `json:"message"`
	Component      string    `json:"component"`
	FirstTimestamp time.Time `json:"first_timestamp"`
	LastTimestamp  time.Time `json:"last_timestamp"`
	Count          int       `json:"count"`
	NodeID         string    `json:"node_id"`
	Type           string    `json:"type"`
}

// eventToLogstash converts the api.Event into a LogstashMessage
func eventToLogstash(evt *api.Event) (lsm *LogstashMessage, err error) {
	lsm = &LogstashMessage{
		Name:           evt.Name,
		Namespace:      evt.Namespace,
		Kind:           evt.InvolvedObject.Kind,
		Reason:         evt.Reason,
		Message:        evt.Message,
		Component:      evt.Source.Component,
		FirstTimestamp: evt.FirstTimestamp.Time,
		LastTimestamp:  evt.LastTimestamp.Time,
		Count:          evt.Count,
		Type:           logstashMessageType,
	}

	if evt.InvolvedObject.Kind == "Pod" {
		lsm.PodName = evt.InvolvedObject.Name
	}

	if nodeID := evt.Source.Host; nodeID != "" {
		lsm.NodeID = nodeID
		return
	}

	if nodeID := string(evt.InvolvedObject.UID); nodeID != "" {
		lsm.NodeID = nodeID
		return
	}

	return
}
