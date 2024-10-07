package debounce

// SimpleDebouncer debounce function call with simple logc:
// 1. If call is currently pending, function call should go through
// 2. If call is scheduled, but not pending, function call should be voided
type SimpleDebouncer struct {
	channel chan struct{}
}

// NewDebouncer creates a new Debouncer with a buffered channel of size 1
func NewSimpleDebouncer() *SimpleDebouncer {
	return &SimpleDebouncer{
		channel: make(chan struct{}, 1),
	}
}

// Debounce attempts to execute the function if the channel allows it
func (d *SimpleDebouncer) Debounce(fn func()) {
	select {
	case d.channel <- struct{}{}:
		fn()
		<-d.channel
	default:
	}
}
