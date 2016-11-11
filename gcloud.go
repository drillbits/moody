package moody

import (
	"context"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

var (
	newCloudClient          = defaultNewCloudClient
	createCloudTopic        = defaultCreateCloudTopic
	cloudTopic              = defaultCloudTopic
	createCloudSubscription = defaultCreateCloudSubscription
	cloudSubscription       = defaultCloudSubscription
)

func defaultNewCloudClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*pubsub.Client, error) {
	return pubsub.NewClient(ctx, projectID, opts...)
}

func defaultCreateCloudTopic(ctx context.Context, client *pubsub.Client, id string) (*pubsub.Topic, error) {
	return client.CreateTopic(ctx, id)
}

func defaultCloudTopic(client *pubsub.Client, id string) *pubsub.Topic {
	return client.Topic(id)
}

func defaultCreateCloudSubscription(ctx context.Context, client *pubsub.Client, id string, topic *pubsub.Topic, ackDeadline time.Duration, pushConfig *pubsub.PushConfig) (*pubsub.Subscription, error) {
	return client.CreateSubscription(ctx, id, topic, ackDeadline, pushConfig)
}

func defaultCloudSubscription(client *pubsub.Client, id string) *pubsub.Subscription {
	return client.Subscription(id)
}

// NewCloudPubSubClient creates a new Google Cloud Pub/Sub client.
func NewCloudPubSubClient(ctx context.Context) (*pubsub.Client, error) {
	cfg := ConfigFromContext(ctx)

	client, err := newCloudClient(ctx, cfg.GCP.ProjectID, option.WithServiceAccountFile(cfg.GCP.ServiceAccountFile))
	if err != nil {
		return nil, err
	}

	return client, nil
}

// CreateTopicIfNotExists creates a new Google Cloud Pub/Sub topic if it doesn't exist.
func CreateTopicIfNotExists(ctx context.Context, client *pubsub.Client, rawid string) (*pubsub.Topic, error) {
	id := url.QueryEscape(rawid)
	topic, err := createCloudTopic(ctx, client, id)
	if err != nil {
		if strings.Contains(err.Error(), "Resource already exists in the project") {
			return cloudTopic(client, id), nil
		}
		return nil, err
	}
	return topic, nil
}

// CreateSubscriptionIfNotExists creates a new Google Cloud Pub/Sub topic if it doesn't exist.
func CreateSubscriptionIfNotExists(ctx context.Context, client *pubsub.Client, rawid string, topic *pubsub.Topic, ackDeadline time.Duration, pushConfig *pubsub.PushConfig) (*pubsub.Subscription, error) {
	id := url.QueryEscape(rawid)
	s, err := createCloudSubscription(ctx, client, id, topic, ackDeadline, pushConfig)
	if err != nil {
		if strings.Contains(err.Error(), "Resource already exists in the project") {
			return cloudSubscription(client, id), nil
		}
		return nil, err
	}
	return s, nil
}
