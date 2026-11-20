//go:build integration

package producer_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/silviolleite/loafer-awsx/conn"
	"github.com/silviolleite/loafer-awsx/idgen"
	"github.com/silviolleite/loafer-awsx/producer"
)

const (
	integrationRegion         = "us-east-1"
	integrationAccessKey      = "test"
	integrationSecretKey      = "test"
	integrationWaitFor        = 30 * time.Second
	integrationPollEvery      = 100 * time.Millisecond
	integrationReceiveTimeout = 5 * time.Second
	integrationMaxBatch       = 10
	integrationMaxAttributes  = 10
	integrationWaitSeconds    = 1
	integrationLargeBody      = 32 * 1024
	integrationMaxGroupID     = 128
)

var integrationNameSeq atomic.Uint64

type integrationFanOut struct {
	sns      *sns.Client
	sqs      *sqs.Client
	producer *producer.Producer
	topicARN string
	queueURL string
}

type integrationReceived struct {
	attributes map[string]string
	dataTypes  map[string]string
	body       string
	groupID    string
	dedupID    string
}

type integrationBatchMessage struct {
	Payload string `json:"payload"`
	Group   string `json:"group"`
	Seq     int    `json:"seq"`
}

type integrationBatch struct {
	order     map[string][]int
	wantDedup map[string]string
	bodies    []string
	entries   []*producer.PublishBatchEntry
}

func integrationEndpoint() string {
	if v := os.Getenv("MINISTACK_ENDPOINT"); v != "" {
		return v
	}
	return "http://localhost:4566"
}

func integrationConfig(t *testing.T) aws.Config {
	t.Helper()

	cfg, err := conn.New(context.Background(),
		conn.WithRegion(integrationRegion),
		conn.WithAccessKey(integrationAccessKey, integrationSecretKey),
		conn.WithEndpoint(integrationEndpoint()),
		conn.WithRetryCount(1),
	)
	require.NoError(t, err)

	transport := awshttp.NewBuildableClient().GetTransport()
	t.Cleanup(transport.CloseIdleConnections)
	cfg.HTTPClient = &http.Client{Transport: transport}

	return cfg
}

func integrationUniqueName(prefix string) string {
	return fmt.Sprintf("loafer-it-producer-%s-%d-%d", prefix, time.Now().UnixNano(), integrationNameSeq.Add(1))
}

func integrationNewFanOut(t *testing.T, fifo bool, opts ...producer.Option) *integrationFanOut {
	t.Helper()

	cfg := integrationConfig(t)
	f := &integrationFanOut{sns: sns.NewFromConfig(cfg), sqs: sqs.NewFromConfig(cfg)}

	p, err := producer.New(f.sns, opts...)
	require.NoError(t, err)
	f.producer = p

	topicName := integrationUniqueName("topic")
	queueName := integrationUniqueName("queue")
	var topicAttrs, queueAttrs map[string]string
	if fifo {
		topicName += ".fifo"
		queueName += ".fifo"
		topicAttrs = map[string]string{"FifoTopic": "true", "ContentBasedDeduplication": "true"}
		queueAttrs = map[string]string{"FifoQueue": "true", "ContentBasedDeduplication": "true"}
	}

	f.topicARN = integrationCreateTopic(t, f.sns, topicName, topicAttrs)
	f.queueURL = integrationCreateQueue(t, f.sqs, queueName, queueAttrs)
	queueARN := integrationQueueARN(t, f.sqs, f.queueURL)
	integrationAllowTopic(t, f.sqs, f.queueURL, queueARN, f.topicARN)
	integrationSubscribe(t, f.sns, f.topicARN, queueARN)

	return f
}

func integrationCreateTopic(t *testing.T, client *sns.Client, name string, attrs map[string]string) string {
	t.Helper()

	out, err := client.CreateTopic(context.Background(), &sns.CreateTopicInput{
		Name:       aws.String(name),
		Attributes: attrs,
	})
	require.NoError(t, err)
	require.NotNil(t, out.TopicArn)

	arn := *out.TopicArn
	t.Cleanup(func() {
		_, err := client.DeleteTopic(context.Background(), &sns.DeleteTopicInput{TopicArn: aws.String(arn)})
		assert.NoError(t, err)
	})

	return arn
}

func integrationCreateQueue(t *testing.T, client *sqs.Client, name string, attrs map[string]string) string {
	t.Helper()

	out, err := client.CreateQueue(context.Background(), &sqs.CreateQueueInput{
		QueueName:  aws.String(name),
		Attributes: attrs,
	})
	require.NoError(t, err)
	require.NotNil(t, out.QueueUrl)

	url := *out.QueueUrl
	t.Cleanup(func() {
		_, err := client.DeleteQueue(context.Background(), &sqs.DeleteQueueInput{QueueUrl: aws.String(url)})
		var notFound *sqstypes.QueueDoesNotExist
		if !errors.As(err, &notFound) {
			assert.NoError(t, err)
		}
	})

	return url
}

func integrationQueueARN(t *testing.T, client *sqs.Client, url string) string {
	t.Helper()

	out, err := client.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(url),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	arn := out.Attributes[string(sqstypes.QueueAttributeNameQueueArn)]
	require.NotEmpty(t, arn)

	return arn
}

func integrationAllowTopic(t *testing.T, client *sqs.Client, url, queueARN, topicARN string) {
	t.Helper()

	policy, err := json.Marshal(map[string]any{
		"Version": "2012-10-17",
		"Statement": []map[string]any{{
			"Effect":    "Allow",
			"Principal": map[string]string{"Service": "sns.amazonaws.com"},
			"Action":    "sqs:SendMessage",
			"Resource":  queueARN,
			"Condition": map[string]any{"ArnEquals": map[string]string{"aws:SourceArn": topicARN}},
		}},
	})
	require.NoError(t, err)

	_, err = client.SetQueueAttributes(context.Background(), &sqs.SetQueueAttributesInput{
		QueueUrl:   aws.String(url),
		Attributes: map[string]string{string(sqstypes.QueueAttributeNamePolicy): string(policy)},
	})
	require.NoError(t, err)
}

func integrationSubscribe(t *testing.T, client *sns.Client, topicARN, queueARN string) {
	t.Helper()

	out, err := client.Subscribe(context.Background(), &sns.SubscribeInput{
		TopicArn:              aws.String(topicARN),
		Protocol:              aws.String("sqs"),
		Endpoint:              aws.String(queueARN),
		Attributes:            map[string]string{"RawMessageDelivery": "true"},
		ReturnSubscriptionArn: true,
	})
	require.NoError(t, err)
	require.NotNil(t, out.SubscriptionArn)

	arn := *out.SubscriptionArn
	t.Cleanup(func() {
		_, err := client.Unsubscribe(context.Background(), &sns.UnsubscribeInput{SubscriptionArn: aws.String(arn)})
		var notFound *snstypes.NotFoundException
		if !errors.As(err, &notFound) {
			assert.NoError(t, err)
		}
	})
}

func (f *integrationFanOut) receive(t *testing.T, want int) []integrationReceived {
	t.Helper()

	var got []integrationReceived
	seen := make(map[string]bool)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		ctx, cancel := context.WithTimeout(context.Background(), integrationReceiveTimeout)
		defer cancel()

		out, err := f.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:              aws.String(f.queueURL),
			MaxNumberOfMessages:   integrationMaxBatch,
			WaitTimeSeconds:       integrationWaitSeconds,
			MessageAttributeNames: []string{"All"},
			MessageSystemAttributeNames: []sqstypes.MessageSystemAttributeName{
				sqstypes.MessageSystemAttributeNameMessageGroupId,
				sqstypes.MessageSystemAttributeNameMessageDeduplicationId,
			},
		})
		require.NoError(c, err)
		for _, m := range out.Messages {
			_, err := f.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(f.queueURL),
				ReceiptHandle: m.ReceiptHandle,
			})
			require.NoError(c, err)
			id := aws.ToString(m.MessageId)
			if !seen[id] {
				seen[id] = true
				got = append(got, integrationConvert(m))
			}
		}
		assert.GreaterOrEqual(c, len(got), want)
	}, integrationWaitFor, integrationPollEvery)

	require.Len(t, got, want)
	return got
}

func integrationConvert(m sqstypes.Message) integrationReceived {
	r := integrationReceived{
		body:       aws.ToString(m.Body),
		groupID:    m.Attributes[string(sqstypes.MessageSystemAttributeNameMessageGroupId)],
		dedupID:    m.Attributes[string(sqstypes.MessageSystemAttributeNameMessageDeduplicationId)],
		attributes: make(map[string]string, len(m.MessageAttributes)),
		dataTypes:  make(map[string]string, len(m.MessageAttributes)),
	}
	for k, v := range m.MessageAttributes {
		r.attributes[k] = aws.ToString(v.StringValue)
		r.dataTypes[k] = aws.ToString(v.DataType)
	}
	return r
}

func integrationByBody(got []integrationReceived) map[string]integrationReceived {
	byBody := make(map[string]integrationReceived, len(got))
	for _, m := range got {
		byBody[m.body] = m
	}
	return byBody
}

func integrationStringTypes(attrs map[string]string) map[string]string {
	dataTypes := make(map[string]string, len(attrs))
	for k := range attrs {
		dataTypes[k] = "String"
	}
	return dataTypes
}

func integrationTenAttributes() map[string]string {
	attrs := make(map[string]string, integrationMaxAttributes)
	for i := range integrationMaxAttributes {
		attrs[fmt.Sprintf("attr-%d", i)] = fmt.Sprintf("value-%d", i)
	}
	return attrs
}

func integrationContentDedupID(body string) string {
	sum := sha256.Sum256([]byte(body))
	return hex.EncodeToString(sum[:])
}

func integrationFNV64ID(canonical string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(canonical))
	return strconv.FormatUint(h.Sum64(), 16)
}

func TestIntegrationPublishStandardFanOut(t *testing.T) {
	cases := []struct {
		attributes map[string]string
		name       string
		body       string
	}{
		{name: "plain body without attributes", body: "hello loafer", attributes: map[string]string{}},
		{name: "json body with one attribute", body: `{"event":"created","id":42}`, attributes: map[string]string{"event-type": "created"}},
		{name: "unicode body", body: "olá 世界 🚀 ünïcødé", attributes: map[string]string{"locale": "pt-BR ✓"}},
		{name: "whitespace body", body: "  line one\n\tline two\r\n  ", attributes: map[string]string{"kind": "text"}},
		{name: "large body with ten attributes", body: strings.Repeat("x", integrationLargeBody), attributes: integrationTenAttributes()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := integrationNewFanOut(t, false)

			id, err := f.producer.Publish(context.Background(), &producer.PublishInput{
				TopicARN:   f.topicARN,
				Message:    tc.body,
				Attributes: tc.attributes,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, id)

			got := f.receive(t, 1)

			assert.Equal(t, tc.body, got[0].body)
			assert.Equal(t, tc.attributes, got[0].attributes)
			assert.Equal(t, integrationStringTypes(tc.attributes), got[0].dataTypes)
			assert.Empty(t, got[0].groupID)
		})
	}
}

func TestIntegrationPublishFIFOFanOut(t *testing.T) {
	cases := []struct {
		attributes map[string]string
		name       string
		body       string
		groupID    string
		dedupID    string
		wantDedup  string
	}{
		{
			name:       "content based deduplication",
			attributes: map[string]string{},
			body:       `{"order":1}`,
			groupID:    "group-a",
			wantDedup:  integrationContentDedupID(`{"order":1}`),
		},
		{
			name:       "attributes",
			attributes: map[string]string{"tenant": "acme", "region": "us-east-1"},
			body:       "fifo with attributes",
			groupID:    "group-b",
			wantDedup:  integrationContentDedupID("fifo with attributes"),
		},
		{
			name:       "explicit deduplication id",
			attributes: map[string]string{},
			body:       "explicit dedup",
			groupID:    "group-c",
			dedupID:    "dedup-explicit-1",
			wantDedup:  "dedup-explicit-1",
		},
		{
			name:       "unicode body with punctuated group id",
			attributes: map[string]string{},
			body:       "olá 世界",
			groupID:    "grupo:a/b.c_1!",
			wantDedup:  integrationContentDedupID("olá 世界"),
		},
		{
			name:       "max length group id",
			attributes: map[string]string{},
			body:       "long group",
			groupID:    strings.Repeat("g", integrationMaxGroupID),
			wantDedup:  integrationContentDedupID("long group"),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := integrationNewFanOut(t, true)

			id, err := f.producer.Publish(context.Background(), &producer.PublishInput{
				TopicARN:        f.topicARN,
				Message:         tc.body,
				GroupID:         tc.groupID,
				DeduplicationID: tc.dedupID,
				Attributes:      tc.attributes,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, id)

			got := f.receive(t, 1)

			assert.Equal(t, tc.body, got[0].body)
			assert.Equal(t, tc.groupID, got[0].groupID)
			assert.Equal(t, tc.wantDedup, got[0].dedupID)
			assert.Equal(t, tc.attributes, got[0].attributes)
			assert.Equal(t, integrationStringTypes(tc.attributes), got[0].dataTypes)
		})
	}
}

func integrationNewBatch(
	t *testing.T,
	size int,
	group, dedup func(int) string,
	wantDedup func(seq int, body string) string,
) integrationBatch {
	t.Helper()

	b := integrationBatch{
		order:     map[string][]int{},
		wantDedup: make(map[string]string, size),
		bodies:    make([]string, 0, size),
		entries:   make([]*producer.PublishBatchEntry, 0, size),
	}
	for seq := range size {
		raw, err := json.Marshal(integrationBatchMessage{Payload: fmt.Sprintf("payload-%d", seq), Group: group(seq), Seq: seq})
		require.NoError(t, err)
		body := string(raw)
		b.entries = append(b.entries, &producer.PublishBatchEntry{
			ID:              fmt.Sprintf("entry-%d", seq),
			Message:         body,
			GroupID:         group(seq),
			DeduplicationID: dedup(seq),
			Attributes:      map[string]string{"seq": strconv.Itoa(seq)},
		})
		b.bodies = append(b.bodies, body)
		b.wantDedup[body] = wantDedup(seq, body)
		b.order[group(seq)] = append(b.order[group(seq)], seq)
	}
	return b
}

func integrationPublishBatch(t *testing.T, f *integrationFanOut, b integrationBatch) map[string][]int {
	t.Helper()

	out, err := f.producer.PublishBatch(context.Background(), &producer.PublishBatchInput{
		TopicARN: f.topicARN,
		Messages: b.entries,
	})
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Empty(t, out.Failed)
	require.Len(t, out.Successful, len(b.entries))

	wantIDs := make([]string, 0, len(b.entries))
	for _, e := range b.entries {
		wantIDs = append(wantIDs, e.ID)
	}
	gotIDs := make([]string, 0, len(out.Successful))
	for _, s := range out.Successful {
		gotIDs = append(gotIDs, s.EntryID)
		assert.NotEmpty(t, s.MessageID)
	}
	assert.ElementsMatch(t, wantIDs, gotIDs)

	got := f.receive(t, len(b.entries))

	gotBodies := make([]string, 0, len(got))
	order := map[string][]int{}
	for _, m := range got {
		var msg integrationBatchMessage
		require.NoError(t, json.Unmarshal([]byte(m.body), &msg))
		assert.Equal(t, msg.Group, m.groupID)
		assert.Equal(t, b.wantDedup[m.body], m.dedupID)
		assert.Equal(t, map[string]string{"seq": strconv.Itoa(msg.Seq)}, m.attributes)
		gotBodies = append(gotBodies, m.body)
		order[m.groupID] = append(order[m.groupID], msg.Seq)
	}
	assert.ElementsMatch(t, b.bodies, gotBodies)

	return order
}

func integrationNoID(int) string { return "" }

func integrationNoDedup(int, string) string { return "" }

func integrationContentDedup(_ int, body string) string { return integrationContentDedupID(body) }

func TestIntegrationPublishBatchStandardFanOut(t *testing.T) {
	cases := []struct {
		name string
		size int
	}{
		{name: "single entry", size: 1},
		{name: "half batch", size: integrationMaxBatch / 2},
		{name: "full batch", size: integrationMaxBatch},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := integrationNewFanOut(t, false)
			b := integrationNewBatch(t, tc.size, integrationNoID, integrationNoID, integrationNoDedup)

			integrationPublishBatch(t, f, b)
		})
	}
}

func TestIntegrationPublishBatchFIFOFanOut(t *testing.T) {
	explicitDedup := func(seq int) string { return fmt.Sprintf("explicit-dedup-%d", seq) }

	cases := []struct {
		group     func(int) string
		dedup     func(int) string
		wantDedup func(int, string) string
		name      string
		size      int
	}{
		{
			name:      "single entry",
			size:      1,
			group:     func(int) string { return "group-single" },
			dedup:     integrationNoID,
			wantDedup: integrationContentDedup,
		},
		{
			name:      "one group",
			size:      integrationMaxBatch / 2,
			group:     func(int) string { return "group-only" },
			dedup:     integrationNoID,
			wantDedup: integrationContentDedup,
		},
		{
			name:      "interleaved groups",
			size:      integrationMaxBatch,
			group:     func(seq int) string { return fmt.Sprintf("group-%d", seq%3) },
			dedup:     integrationNoID,
			wantDedup: integrationContentDedup,
		},
		{
			name:      "explicit deduplication ids",
			size:      integrationMaxBatch,
			group:     func(seq int) string { return fmt.Sprintf("group-%d", seq%2) },
			dedup:     explicitDedup,
			wantDedup: func(seq int, _ string) string { return explicitDedup(seq) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := integrationNewFanOut(t, true)
			b := integrationNewBatch(t, tc.size, tc.group, tc.dedup, tc.wantDedup)

			order := integrationPublishBatch(t, f, b)

			assert.Equal(t, b.order, order)
		})
	}
}

func integrationGeneratorAttributes(tenant string, seq int) map[string]string {
	return map[string]string{"tenant": tenant, "order": strconv.Itoa(seq), "ignored": "x"}
}

func TestIntegrationPublishFIFOGeneratedIDs(t *testing.T) {
	longTenant := strings.Repeat("t", integrationMaxGroupID)

	cases := []struct {
		gen    idgen.GroupIDGenerator
		want   func(seq int) string
		name   string
		tenant string
	}{
		{
			name:   "key based sha256",
			gen:    idgen.NewKeyBased(idgen.WithFields("tenant", "order")),
			tenant: "acme",
			want:   func(seq int) string { return integrationContentDedupID(fmt.Sprintf("order:%d:tenant:acme", seq)) },
		},
		{
			name:   "key based fnv64",
			gen:    idgen.NewKeyBased(idgen.WithHashAlgorithm(idgen.FNV64), idgen.WithFields("tenant", "order")),
			tenant: "acme",
			want:   func(seq int) string { return integrationFNV64ID(fmt.Sprintf("order:%d:tenant:acme", seq)) },
		},
		{
			name:   "key based all fields",
			gen:    idgen.NewKeyBased(),
			tenant: "acme",
			want: func(seq int) string {
				return integrationContentDedupID(fmt.Sprintf("ignored:x:order:%d:tenant:acme", seq))
			},
		},
		{
			name:   "composite",
			gen:    idgen.NewComposite(idgen.WithFields("tenant", "order")),
			tenant: "acme",
			want:   func(seq int) string { return fmt.Sprintf("acme:%d", seq) },
		},
		{
			name:   "composite custom separator",
			gen:    idgen.NewComposite(idgen.WithSeparator("_"), idgen.WithFields("tenant", "order")),
			tenant: "acme",
			want:   func(seq int) string { return fmt.Sprintf("acme_%d", seq) },
		},
		{
			name:   "key based long values",
			gen:    idgen.NewKeyBased(idgen.WithFields("tenant", "order")),
			tenant: longTenant,
			want: func(seq int) string {
				return integrationContentDedupID(fmt.Sprintf("order:%d:tenant:%s", seq, longTenant))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := integrationNewFanOut(t, true, producer.WithGroupIDGenerator(tc.gen), producer.WithDeduplicationIDGenerator(tc.gen))

			attributes := []map[string]string{
				integrationGeneratorAttributes(tc.tenant, 0),
				integrationGeneratorAttributes(tc.tenant, 1),
				integrationGeneratorAttributes(tc.tenant, 2),
			}
			want := make(map[string]string, len(attributes))
			for i := range attributes {
				want[fmt.Sprintf("generated-%d", i)] = tc.want(i)
			}

			_, err := f.producer.Publish(context.Background(), &producer.PublishInput{
				TopicARN:   f.topicARN,
				Message:    "generated-0",
				Attributes: attributes[0],
			})
			require.NoError(t, err)
			out, err := f.producer.PublishBatch(context.Background(), &producer.PublishBatchInput{
				TopicARN: f.topicARN,
				Messages: []*producer.PublishBatchEntry{
					{ID: "one", Message: "generated-1", Attributes: attributes[1]},
					{ID: "two", Message: "generated-2", Attributes: attributes[2]},
				},
			})
			require.NoError(t, err)
			assert.Empty(t, out.Failed)
			assert.Len(t, out.Successful, 2)

			got := integrationByBody(f.receive(t, len(attributes)))

			require.Len(t, got, len(want))
			for body, id := range want {
				assert.Equal(t, id, got[body].groupID, body)
				assert.Equal(t, id, got[body].dedupID, body)
			}
		})
	}
}

func TestIntegrationPublishFIFORandomGeneratedIDs(t *testing.T) {
	const uuidPattern = `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`

	cases := []struct {
		groupGen     idgen.GroupIDGenerator
		dedupGen     idgen.DeduplicationIDGenerator
		name         string
		groupPattern string
	}{
		{
			name:         "random group and deduplication ids",
			groupGen:     idgen.NewRandom(),
			dedupGen:     idgen.NewRandom(),
			groupPattern: uuidPattern,
		},
		{
			name:         "composite with suffix group id",
			groupGen:     idgen.NewCompositeWithSuffix(idgen.WithFields("tenant"), idgen.WithSuffixRange(1, 5)),
			dedupGen:     idgen.NewRandom(),
			groupPattern: `^acme:[1-5]$`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := integrationNewFanOut(t, true, producer.WithGroupIDGenerator(tc.groupGen), producer.WithDeduplicationIDGenerator(tc.dedupGen))
			bodies := []string{"random-0", "random-1", "random-2"}

			_, err := f.producer.Publish(context.Background(), &producer.PublishInput{
				TopicARN:   f.topicARN,
				Message:    bodies[0],
				Attributes: integrationGeneratorAttributes("acme", 0),
			})
			require.NoError(t, err)
			out, err := f.producer.PublishBatch(context.Background(), &producer.PublishBatchInput{
				TopicARN: f.topicARN,
				Messages: []*producer.PublishBatchEntry{
					{ID: "one", Message: bodies[1], Attributes: integrationGeneratorAttributes("acme", 1)},
					{ID: "two", Message: bodies[2], Attributes: integrationGeneratorAttributes("acme", 2)},
				},
			})
			require.NoError(t, err)
			assert.Empty(t, out.Failed)
			assert.Len(t, out.Successful, 2)

			got := integrationByBody(f.receive(t, len(bodies)))

			require.Len(t, got, len(bodies))
			dedupIDs := make(map[string]struct{}, len(bodies))
			for _, body := range bodies {
				assert.Regexp(t, tc.groupPattern, got[body].groupID, body)
				assert.Regexp(t, uuidPattern, got[body].dedupID, body)
				dedupIDs[got[body].dedupID] = struct{}{}
			}
			assert.Len(t, dedupIDs, len(bodies))
		})
	}
}

func TestIntegrationPublishGeneratorPrecedence(t *testing.T) {
	cases := []struct {
		group func(int) string
		dedup func(int) string
		name  string
		fifo  bool
	}{
		{
			name:  "fifo explicit ids win over generators",
			fifo:  true,
			group: func(seq int) string { return fmt.Sprintf("explicit-group-%d", seq) },
			dedup: func(seq int) string { return fmt.Sprintf("explicit-dedup-%d", seq) },
		},
		{
			name:  "standard topic ignores generators",
			group: integrationNoID,
			dedup: integrationNoID,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := integrationNewFanOut(t, tc.fifo,
				producer.WithGroupIDGenerator(idgen.NewRandom()),
				producer.WithDeduplicationIDGenerator(idgen.NewRandom()),
			)
			bodies := []string{"precedence-0", "precedence-1", "precedence-2"}

			_, err := f.producer.Publish(context.Background(), &producer.PublishInput{
				TopicARN:        f.topicARN,
				Message:         bodies[0],
				GroupID:         tc.group(0),
				DeduplicationID: tc.dedup(0),
				Attributes:      integrationGeneratorAttributes("acme", 0),
			})
			require.NoError(t, err)
			entries := make([]*producer.PublishBatchEntry, 0, len(bodies)-1)
			for seq := 1; seq < len(bodies); seq++ {
				entries = append(entries, &producer.PublishBatchEntry{
					ID:              fmt.Sprintf("entry-%d", seq),
					Message:         bodies[seq],
					GroupID:         tc.group(seq),
					DeduplicationID: tc.dedup(seq),
					Attributes:      integrationGeneratorAttributes("acme", seq),
				})
			}
			out, err := f.producer.PublishBatch(context.Background(), &producer.PublishBatchInput{
				TopicARN: f.topicARN,
				Messages: entries,
			})
			require.NoError(t, err)
			assert.Empty(t, out.Failed)
			assert.Len(t, out.Successful, 2)

			got := integrationByBody(f.receive(t, len(bodies)))

			require.Len(t, got, len(bodies))
			for seq, body := range bodies {
				assert.Equal(t, tc.group(seq), got[body].groupID, body)
				assert.Equal(t, tc.dedup(seq), got[body].dedupID, body)
			}
		})
	}
}
