package moody

import "context"

// Broker is the interface wraps moody broker.
type Broker interface {
	Close() error
	InitializeTopics(ctx context.Context, topics []string) (map[string]*Topic, error)
	SubscribeCloud(ctx context.Context, errChan chan error)
	SubscribeLocal(ctx context.Context, errChan chan error)
}
