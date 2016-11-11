package moody

import "encoding/json"

// Message wraps a Pub/Sub message.
type Message struct {
	Data []byte `json:"__message-via-moody___"`
}

// NewMessage creates a new Message.
func NewMessage(b []byte) (*Message, error) {
	m := &Message{}
	err := json.Unmarshal(b, m)
	if err != nil {
		return nil, err
	}
	return m, nil
}

// MessageIsViaMoody reports whether bytes are Message from cloud.
func MessageIsViaMoody(b []byte) bool {
	m := &Message{}
	err := json.Unmarshal(b, m)
	return err == nil
}
