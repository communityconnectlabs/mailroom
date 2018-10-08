package waits

import (
	"fmt"
	"time"

	"github.com/nyaruka/goflow/flows"
	"github.com/nyaruka/goflow/flows/resumes"
	"github.com/nyaruka/goflow/utils"
)

var registeredTypes = map[string](func() flows.Wait){}

// RegisterType registers a new type of wait
func RegisterType(name string, initFunc func() flows.Wait) {
	registeredTypes[name] = initFunc
}

// the base of all wait types
type baseWait struct {
	Timeout_   *int       `json:"timeout,omitempty"`
	TimeoutOn_ *time.Time `json:"timeout_on,omitempty"`
}

// Timeout returns the timeout of this wait in seconds or nil if no timeout is set
func (w *baseWait) Timeout() *int { return w.Timeout_ }

// TimeoutOn returns when this wait times out
func (w *baseWait) TimeoutOn() *time.Time { return w.TimeoutOn_ }

// Begin beings waiting
func (w *baseWait) Begin(run flows.FlowRun) {
	if w.Timeout_ != nil {
		timeoutOn := utils.Now().Add(time.Second * time.Duration(*w.Timeout_))

		w.TimeoutOn_ = &timeoutOn
	}
}

// End ends this wait or returns an error
func (w *baseWait) End(resume flows.Resume) error {
	switch resume.Type() {
	case resumes.TypeRunExpiration:
		// expired runs always end a wait
		return nil
	case resumes.TypeWaitTimeout:
		if w.Timeout() == nil || w.TimeoutOn() == nil {
			return fmt.Errorf("can only be applied when session wait has timeout")
		}
		if utils.Now().Before(*w.TimeoutOn()) {
			return fmt.Errorf("can't apply before wait has timed out")
		}
	}
	return nil
}

//------------------------------------------------------------------------------------------
// JSON Encoding / Decoding
//------------------------------------------------------------------------------------------

// ReadWait reads a wait from the given typed envelope
func ReadWait(envelope *utils.TypedEnvelope) (flows.Wait, error) {
	f := registeredTypes[envelope.Type]
	if f == nil {
		return nil, fmt.Errorf("unknown type: %s", envelope.Type)
	}

	wait := f()
	return wait, utils.UnmarshalAndValidate(envelope.Data, wait)
}
