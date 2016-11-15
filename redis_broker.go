package moody

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/garyburd/redigo/redis"
	"google.golang.org/api/iterator"
)

// RedisBroker is a broker using redis.
type RedisBroker struct {
	pool   *redis.Pool
	cloudC *pubsub.Client
	topics map[string]*Topic
	l      sync.RWMutex
}

// NewRedisBroker creates a new RedisBroker.
func NewRedisBroker(ctx context.Context) (*RedisBroker, error) {
	cfg := ConfigFromContext(ctx)

	maxIdle := 10
	pool := redis.NewPool(func() (redis.Conn, error) {
		c, err := redis.DialURL(cfg.RedisURI)
		if err != nil {
			return nil, err
		}
		return c, err
	}, maxIdle)

	cloudC, err := NewCloudPubSubClient(ctx)
	if err != nil {
		return nil, err
	}

	b := &RedisBroker{
		pool:   pool,
		cloudC: cloudC,
	}

	return b, nil
}

// Close closes all connection.
func (b *RedisBroker) Close() error {
	if err := b.pool.Close(); err != nil {
		return err
	}
	if err := b.cloudC.Close(); err != nil {
		return err
	}
	return nil
}

// InitializeTopics initializes topics of Cloud Pub/Sub.
func (b *RedisBroker) InitializeTopics(ctx context.Context, topics []string) (map[string]*Topic, error) {
	m := make(map[string]*Topic)
	cc := b.cloudC
	for _, rawid := range topics {
		t := &Topic{ID: rawid}

		topic, err := CreateTopicIfNotExists(ctx, cc, rawid)
		if err != nil {
			return nil, err
		}
		t.cloudTopic = topic

		sub, err := CreateSubscriptionIfNotExists(ctx, cc, rawid+"/moody", topic, 10*time.Second, nil)
		if err != nil {
			return nil, err
		}
		t.cloudSub = sub

		m[t.ID] = t
	}
	b.topics = m
	return m, nil
}

// SubscribeCloud subscribes Cloud Pub/Sub and receives messages.
// Messages are emitted to local broker if they are not from local.
func (b *RedisBroker) SubscribeCloud(ctx context.Context, errChan chan error) {
	// Subscribe all topics
	for rawid := range b.topics {
		topic, ok := b.topics[rawid]
		if !ok {
			errChan <- fmt.Errorf("topic %s not found", rawid)
		}
		go func(ctx context.Context, sub *pubsub.Subscription) {
			it, err := sub.Pull(ctx)
			if err != nil {
				errChan <- err
				return
			}
			defer it.Stop()

			for {
				m, err := it.Next()
				if err == iterator.Done {
					break
				} else if err != nil {
					errChan <- err
					break
				}
				log.Printf("SubscribeCloud: Receive from cloud topic: %s, msg: %s", topic.ID, string(m.Data))

				if MessageIsViaMoody(m.Data) {
					// Do not emit a message via moody
					log.Println("SubscribeCloud: Pass because via moody")
					m.Done(true)
					continue
				}

				// TODO: make a function
				msg := &Message{Data: m.Data}
				err = b.EmitLocal(ctx, topic.ID, msg)
				if err != nil {
					// TODO: Normal Pub/Sub
					log.Printf("SubscribeCloud: Error on EmitLocal: %s", err)
					errChan <- err
				} else {
					log.Printf("SubscribeCloud: Emit to local topic: %s, data: %v\n", topic.ID, m.Data)
				}
				m.Done(true)
			}
		}(ctx, topic.cloudSub)
	}
}

// SubscribeLocal subscribes local broker and receives messages.
// Messages are emitted to Cloud Pub/Sub if they are not from cloud.
func (b *RedisBroker) SubscribeLocal(ctx context.Context, errChan chan error) {
	c := b.pool.Get()
	defer c.Close()

	psc := redis.PubSubConn{Conn: c}
	// Subscribe all topics
	for rawid := range b.topics {
		err := psc.Subscribe(rawid)
		if err != nil {
			errChan <- err
			return
		}
		log.Printf("SubscribeLocal: Subscribe local topic: %s", rawid)
	}
	// Receive
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			log.Printf("SubscribeLocal: Receive from local topic: %s, msg: %s", v.Channel, string(v.Data))

			if MessageIsViaMoody(v.Data) {
				// Do not emit a message via moody
				log.Println("SubscribeLocal: Pass because via moody")
				continue
			}

			// TODO: make a function
			msg := &Message{Data: v.Data}
			msgIDs, err := b.EmitCloud(ctx, v.Channel, msg)
			if err != nil {
				log.Printf("SubscribeLocal: Error on emitCloud: %s", err)
				errChan <- err
				continue
			}
			log.Printf("SubscribeLocal: Emit to cloud topic: %s, data: %v, msgIDs: %v\n", v.Channel, v.Data, msgIDs)
		case error:
			errChan <- v
		}
	}
}

// EmitCloud emits a Message to Cloud Pub/Sub.
func (b *RedisBroker) EmitCloud(ctx context.Context, rawid string, msg *Message) ([]string, error) {
	b.l.RLock()
	defer b.l.RUnlock()

	topic, ok := b.topics[rawid]
	if !ok {
		return nil, fmt.Errorf("topic %s not found", rawid)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	msgIDs, err := topic.PublishCloud(ctx, &pubsub.Message{
		Data: data,
	})
	if err != nil {
		return nil, err
	}

	return msgIDs, nil
}

// EmitLocal emits a Message to local.
func (b *RedisBroker) EmitLocal(ctx context.Context, rawid string, msg *Message) error {
	b.l.RLock()
	defer b.l.RUnlock()

	c := b.pool.Get()
	defer c.Close()

	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	_, err = c.Do("PUBLISH", rawid, data)
	if err != nil {
		return err
	}

	return nil
}
