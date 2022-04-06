package transport

// Message abstracts a message object to be used by different transport components.
type Message struct {
	ID          string `json:"id"`
	MsgType     string `json:"msgType"`
	Version     string `json:"version"`
	Destination string `json:"destination"`
	Payload     []byte `json:"payload"`
}
