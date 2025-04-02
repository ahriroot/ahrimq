package v1

type DroppingChan[T any] struct {
	ch chan T
}

func NewDroppingChan[T any](size int) *DroppingChan[T] {
	return &DroppingChan[T]{
		ch: make(chan T, size),
	}
}

func (dc *DroppingChan[T]) Send(value T) {
	select {
	case dc.ch <- value:
	default:
		<-dc.ch
		dc.ch <- value
	}
}

func (dc *DroppingChan[T]) Receive() T {
	return <-dc.ch
}
