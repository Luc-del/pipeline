package pipeline

import (
	"context"
	"sync"
)

type Node[T any] func(T) (T, error)
type Publisher[T any] func() (T, error)
type Receiver[T any] func(T) error

type Config[T any] struct {
	publisher Publisher[T]
	nodes     []Node[T]
	receiver  Receiver[T]
}

type Pipeline[T any] struct {
	wg     *sync.WaitGroup
	cancel context.CancelFunc
	cfg    Config[T]
}

func NewPipeline[T any](cfg Config[T]) Pipeline[T] {
	wg := sync.WaitGroup{}
	wg.Add(len(cfg.nodes) + 2)

	return Pipeline[T]{
		wg:  &wg,
		cfg: cfg,
	}
}

// Receiver <-- Node <-- ... <-- Node <-- Publisher
func (p Pipeline[T]) Run(ctx context.Context) {
	var cl context.CancelFunc
	ctx, cl = context.WithCancel(ctx)
	p.cancel = cl
	read := make(chan T)

	n := receiverNode[T]{
		Receiver: p.cfg.receiver,
		cl:       cl,
		callback: p.wg.Done,
	}
	go n.read(ctx, read)

	for i := len(p.cfg.nodes) - 1; i >= 0; i-- {
		ctx, cl = context.WithCancel(ctx)
		var write chan T
		write, read = read, make(chan T)

		n := node[T]{
			Node:     p.cfg.nodes[i],
			cl:       cl,
			write:    write,
			callback: p.wg.Done,
		}
		go n.read(ctx, read)
	}

	pub := publisherNode[T]{
		Publisher: p.cfg.publisher,
		write:     read,
		callback:  p.wg.Done,
	}
	go pub.read(ctx)

	p.wg.Wait()
}

func (p Pipeline[T]) Cancel() {
	p.cancel()
}
