package events

import (
	"context"
	"sort"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/argoproj/argo-workflows/v3/util/env"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

// by default, allow a source to send 10000 events about an object
const defaultSpamBurst = 10000

// defaultEventRecorderCacheSize limits memory growth from event recorders.
// Each entry holds an EventBroadcaster with goroutines.
// 1024 supports large multi-tenant clusters while bounding resource usage.
const defaultEventRecorderCacheSize = 50

type EventRecorderManager interface {
	Get(ctx context.Context, namespace string) record.EventRecorder
}

type eventRecorderEntry struct {
	broadcaster record.EventBroadcaster
	recorder    record.EventRecorder
}

type eventRecorderManager struct {
	kubernetes     kubernetes.Interface
	lock           sync.Mutex
	eventRecorders *lru.Cache[string, *eventRecorderEntry]
}

// customEventAggregatorFuncWithAnnotations enhances the default `EventAggregatorByReasonFunc` by
// including annotation values as part of the event aggregation key.
func customEventAggregatorFuncWithAnnotations(event *apiv1.Event) (string, string) {
	var joinedAnnotationsStr string
	includeAnnotations := env.LookupEnvStringOr("EVENT_AGGREGATION_WITH_ANNOTATIONS", "false")
	if annotations := event.GetAnnotations(); includeAnnotations == "true" && annotations != nil {
		annotationVals := make([]string, 0, len(annotations))
		for _, v := range annotations {
			annotationVals = append(annotationVals, v)
		}
		sort.Strings(annotationVals)
		joinedAnnotationsStr = strings.Join(annotationVals, "")
	}
	return strings.Join([]string{
		event.Source.Component,
		event.Source.Host,
		event.InvolvedObject.Kind,
		event.InvolvedObject.Namespace,
		event.InvolvedObject.Name,
		string(event.InvolvedObject.UID),
		event.InvolvedObject.APIVersion,
		event.Type,
		event.Reason,
		event.ReportingController,
		event.ReportingInstance,
		joinedAnnotationsStr,
	},
		""), event.Message
}

func (m *eventRecorderManager) Get(ctx context.Context, namespace string) record.EventRecorder {
	m.lock.Lock()
	defer m.lock.Unlock()
	if entry, ok := m.eventRecorders.Get(namespace); ok {
		return entry.recorder
	}

	setupKlogAdapter(ctx)

	eventCorrelationOption := record.CorrelatorOptions{BurstSize: defaultSpamBurst, KeyFunc: customEventAggregatorFuncWithAnnotations}
	eventBroadcaster := record.NewBroadcasterWithCorrelatorOptions(eventCorrelationOption)

	eventBroadcaster.StartStructuredLogging(klog.Level(0)) // Info level
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: m.kubernetes.CoreV1().Events(namespace)})
	entry := &eventRecorderEntry{
		broadcaster: eventBroadcaster,
		recorder:    eventBroadcaster.NewRecorder(scheme.Scheme, apiv1.EventSource{Component: "workflow-controller"}),
	}
	m.eventRecorders.Add(namespace, entry)
	return entry.recorder
}

func NewEventRecorderManager(kubernetes kubernetes.Interface) EventRecorderManager {
	cache, _ := lru.NewWithEvict[string, *eventRecorderEntry](defaultEventRecorderCacheSize, func(_ string, entry *eventRecorderEntry) {
		entry.broadcaster.Shutdown()
	})
	return &eventRecorderManager{
		kubernetes:     kubernetes,
		lock:           sync.Mutex{},
		eventRecorders: cache,
	}
}
