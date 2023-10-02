package pipeline

import "context"

type receiverNode[T any] struct {
	Receiver[T]

	cl       context.CancelFunc
	callback func()
}

func (n receiverNode[T]) kill() {
	n.cl()
	n.callback()
}

func (n receiverNode[T]) read(ctx context.Context, read <-chan T) {
	defer n.kill()

	for {
		select {
		case <-ctx.Done():
			return

		case data, ok := <-read:
			if !ok {
				return
			}

			if err := n.Receiver(data); err != nil {
				return
			}
		}
	}

}

type publisherNode[T any] struct {
	Publisher[T]

	write    chan<- T
	callback func()
}

func (n publisherNode[T]) kill() {
	close(n.write)
	n.callback()
}

func (n publisherNode[T]) read(ctx context.Context) {
	defer n.kill()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			data, err := n.Publisher()
			if err != nil {
				return
			}

			select {
			case <-ctx.Done():
				return
			case n.write <- data:
			}
		}
	}

}

type node[T any] struct {
	Node[T]

	cl       context.CancelFunc // cl is used to notify upstream nodes
	write    chan<- T           // write is used to notify downstream nodes when closed
	callback func()             // callback is used to notify pipeline that this node has stopped
}

func (n node[T]) kill() {
	n.cl()
	close(n.write)
	n.callback()
}

func (n node[T]) read(ctx context.Context, read <-chan T) {
	defer n.kill()

	for {
		// Ensure the node can be cancelled both downstream and upstream
		select {
		case <-ctx.Done():
			return
		case data, ok := <-read:
			if !ok {
				return
			}

			data, err := n.Node(data)
			if err != nil {
				return
			}

			// Ensure receiver node upstream is listening
			select {
			case <-ctx.Done():
				return
			case n.write <- data:
			}
		}
	}

}
