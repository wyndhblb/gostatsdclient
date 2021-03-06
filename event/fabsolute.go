package event

import (
	"fmt"
	"sync"
	"time"
)

// Absolute is a metric that is not averaged/aggregated.
// We keep each value distinct and then we flush them all individually.
type FAbsolute struct {
	Name   string
	mu     sync.Mutex
	Values []float64
}

func (e *FAbsolute) StatClass() string {
	return "counter"
}

// Update the event with metrics coming from a new one of the same type and with the same key
func (e *FAbsolute) Update(e2 Event) error {
	if e.Type() != e2.Type() {
		return fmt.Errorf("statsd event type conflict: %s vs %s ", e.String(), e2.String())
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.Values = append(e.Values, e2.Payload().([]float64)...)
	return nil
}

//Reset the value
func (e *FAbsolute) Reset() {
	e.Values = []float64{}
}

// Payload returns the aggregated value for this event
func (e FAbsolute) Payload() interface{} {
	return e.Values
}

// Stats returns an array of StatsD events as they travel over UDP
func (e FAbsolute) Stats(tick time.Duration) []string {
	ret := make([]string, 0, len(e.Values))
	for v := range e.Values {
		ret = append(ret, fmt.Sprintf("%s:%g|c", e.Name, v))
	}
	return ret
}

// Key returns the name of this metric
func (e FAbsolute) Key() string {
	return e.Name
}

// SetKey sets the name of this metric
func (e *FAbsolute) SetKey(key string) {
	e.Name = key
}

// Type returns an integer identifier for this type of metric
func (e FAbsolute) Type() int {
	return EventFAbsolute
}

// TypeString returns a name for this type of metric
func (e FAbsolute) TypeString() string {
	return "FAbsolute"
}

// String returns a debug-friendly representation of this metric
func (e FAbsolute) String() string {
	return fmt.Sprintf("{Type: %s, Key: %s, Values: %v}", e.TypeString(), e.Name, e.Values)
}
