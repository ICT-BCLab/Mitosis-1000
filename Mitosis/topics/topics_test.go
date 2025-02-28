package topics

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var topicTest = []struct {
	idx     int
	topic   Topic
	tbyte   byte
	tstring string
}{
	{1, Version, Topics[int(Version)].Bytes()[0], "version"},
	{2, Topic(255), byte(255), "unknown"},
}

func TestKyrin(t *testing.T) {
	b := Topics[int(ShardChallenge)].Bytes()[0]
	fmt.Println(b)
}

func TestTopicRepresentation(t *testing.T) {
	for _, tt := range topicTest {
		if !assert.Equal(t, tt.tstring, tt.topic.String()) {
			assert.FailNowf(t, "errors on topic String()", "idx: %d", tt.idx)
		}
		topic := StringToTopic(tt.tstring)
		if int(topic) > len(Topics) && !assert.Equal(t, tt.topic, topic) {
			assert.FailNowf(t, "errors on string representation of topic", "idx: %d", tt.idx)
		}
		buf := bytes.NewBufferString("xxxxx")
		assert.NoError(t, Prepend(buf, tt.topic))
		if !assert.Equal(t, buf.Bytes()[0], tt.tbyte) {
			assert.FailNowf(t, "error on prepending", "idx: %d", tt.idx)
		}
		topic, err := Extract(buf)
		assert.NoError(t, err)
		assert.Equal(t, tt.topic, topic)
	}
}

func TestCheckConsistency(t *testing.T) {
	tpcs := make([]topicBuf, 0)
	tpcs = append(tpcs, Topics[0])
	tpcs = append(tpcs, Topics[1])

	// consistency check should not trigger if the order of topicBuf array is
	// consistent with that of Topic enum
	assert.NotPanics(t, func() { checkConsistency(tpcs) })

	// skipping an index will result in a panic
	tpcs = append(tpcs, Topics[3])
	assert.Panics(t, func() { checkConsistency(tpcs) })
}
