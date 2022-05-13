package deck

import "time"

type Option interface {
	apply(*option)
}

type OptionFunc func(option *option)

func (o OptionFunc) apply(option *option) {
	o(option)
}

type option struct {
	storage    Storage
	interval   time.Duration
	showLogger bool
}

func WithStorage(storage Storage) OptionFunc {
	return func(option *option) {
		option.storage = storage
	}
}

func WithInterval(interval time.Duration) OptionFunc {
	return func(option *option) {
		option.interval = interval
	}
}
