package proto

import (
	"time"

	"github.com/nats-io/nuid"
	"github.com/nzlov/centrifugo/libcentrifugo/raw"
)

// NewClientInfo allows to initialize ClientInfo.
func NewClientInfo(user string, client string, defaultInfo raw.Raw, channelInfo raw.Raw) *ClientInfo {
	return &ClientInfo{
		User:        user,
		Client:      client,
		DefaultInfo: defaultInfo,
		ChannelInfo: channelInfo,
	}
}

// NewMessage initializes new Message.
func NewMessage(ch string, data []byte, client string, info *ClientInfo) *Message {
	return newMessage(ch, data, client, info, "")
}

// NewMessageWithUID initializes new Message with specified uid.
func NewMessageWithUID(ch string, data []byte, client string, info *ClientInfo, uid string) *Message {
	return newMessage(ch, data, client, info, uid)
}

func newMessage(ch string, data []byte, client string, info *ClientInfo, uid string) *Message {
	if uid == "" {
		uid = nuid.Next()
	}

	raw := raw.Raw(data)
	return &Message{
		UID:       uid,
		Info:      info,
		Channel:   ch,
		Timestamp: time.Now().UnixNano(),
		Data:      raw,
		Client:    client,
	}
}

// NewJoinMessage initializes new JoinMessage.
func NewJoinMessage(ch string, info ClientInfo) *JoinMessage {
	return &JoinMessage{
		Channel: ch,
		Data:    info,
	}
}

// NewLeaveMessage initializes new LeaveMessage.
func NewLeaveMessage(ch string, info ClientInfo) *LeaveMessage {
	return &LeaveMessage{
		Channel: ch,
		Data:    info,
	}
}

// NewControlMessage initializes new ControlMessage.
func NewControlMessage(uid string, method string, params []byte) *ControlMessage {
	raw := raw.Raw(params)
	return &ControlMessage{
		UID:    uid,
		Method: method,
		Params: raw,
	}
}

// NewAdminMessage initializes new AdminMessage.
func NewAdminMessage(method string, params []byte) *AdminMessage {
	raw := raw.Raw(params)
	return &AdminMessage{
		Method: method,
		Params: raw,
	}
}
