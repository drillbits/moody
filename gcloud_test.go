package moody

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"google.golang.org/api/option"
)

func TestNewGCPubSubClient(t *testing.T) {
	ctx := context.Background()
	projectID := "test-project"
	saFile := "./sa.json"
	cfg := &Config{
		GCP: &GCPConfig{
			ProjectID:          projectID,
			ServiceAccountFile: saFile,
		},
	}
	ctx = NewContext(ctx, cfg)
	orgFunc := newCloudClient
	defer func() {
		newCloudClient = orgFunc
	}()

	var argProjectID string
	var argOpt option.ClientOption
	newCloudClient = func(ctx context.Context, projectID string, opts ...option.ClientOption) (*pubsub.Client, error) {
		argProjectID = projectID
		argOpt = opts[0]
		return nil, nil
	}
	_, err := NewCloudPubSubClient(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if argProjectID != projectID {
		t.Errorf("expected: %s, but got: %s", projectID, argProjectID)
	}
	if fmt.Sprint(argOpt) != saFile {
		t.Errorf("expected: %s, but got: %s", saFile, argOpt)
	}
}

func TestCreateTopicIfNotExists(t *testing.T) {
	ctx := context.Background()
	orgFunc1 := createCloudTopic
	orgFunc2 := cloudTopic
	defer func() {
		createCloudTopic = orgFunc1
		cloudTopic = orgFunc2
	}()

	var argID string
	rawid := "test/topic"
	id := url.QueryEscape(rawid)
	createCloudTopic = func(ctx context.Context, client *pubsub.Client, id string) (*pubsub.Topic, error) {
		argID = id
		return &pubsub.Topic{}, nil
	}
	_, err := CreateTopicIfNotExists(ctx, nil, rawid)
	if err != nil {
		t.Fatal(err)
	}
	if argID != id {
		t.Errorf("expected: %s, but got: %s", id, argID)
	}

	createCloudTopic = func(ctx context.Context, client *pubsub.Client, id string) (*pubsub.Topic, error) {
		return nil, errors.New("rpc error: code = 6 desc = Resource already exists in the project")
	}
	cloudTopic = func(client *pubsub.Client, id string) *pubsub.Topic {
		argID = id
		return nil
	}
	_, err = CreateTopicIfNotExists(ctx, nil, rawid)
	if err != nil {
		t.Fatal(err)
	}
	if argID != id {
		t.Errorf("expected: %s, but got: %s", id, argID)
	}
}

func TestCreateSubscriptionIfNotExists(t *testing.T) {
	ctx := context.Background()
	orgFunc1 := createCloudSubscription
	orgFunc2 := cloudSubscription
	defer func() {
		createCloudSubscription = orgFunc1
		cloudSubscription = orgFunc2
	}()
	topic := &pubsub.Topic{}

	var argID string
	rawid := "test/subscription"
	id := url.QueryEscape(rawid)
	createCloudSubscription = func(ctx context.Context, client *pubsub.Client, id string, topic *pubsub.Topic, ackDeadline time.Duration, pushConfig *pubsub.PushConfig) (*pubsub.Subscription, error) {
		argID = id
		return &pubsub.Subscription{}, nil
	}
	_, err := CreateSubscriptionIfNotExists(ctx, nil, rawid, topic, 10*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if argID != id {
		t.Errorf("expected: %s, but got: %s", id, argID)
	}

	createCloudSubscription = func(ctx context.Context, client *pubsub.Client, id string, topic *pubsub.Topic, ackDeadline time.Duration, pushConfig *pubsub.PushConfig) (*pubsub.Subscription, error) {
		return nil, errors.New("rpc error: code = 6 desc = Resource already exists in the project")
	}
	cloudSubscription = func(client *pubsub.Client, id string) *pubsub.Subscription {
		argID = id
		return nil
	}
	_, err = CreateSubscriptionIfNotExists(ctx, nil, rawid, topic, 10*time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if argID != id {
		t.Errorf("expected: %s, but got: %s", id, argID)
	}
}
