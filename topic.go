package moody

import (
	"context"

	"cloud.google.com/go/pubsub"
)

// TestTopics is an array of topic for test.
var TestTopics = []string{
	"test/moody/topic1",
	"test/moody/topic2",
}

// Topic wraps topic and subscription.
type Topic struct {
	ID         string
	cloudTopic *pubsub.Topic
	cloudSub   *pubsub.Subscription
}

// PublishCloud publishes messages to Cloud Pub/Sub.
func (t *Topic) PublishCloud(ctx context.Context, msgs ...*pubsub.Message) ([]string, error) {
	return t.cloudTopic.Publish(ctx, msgs...)
}
