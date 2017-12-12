package enginemgo

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/nzlov/centrifugo/libcentrifugo/channel"
	"github.com/nzlov/centrifugo/libcentrifugo/config"
	"github.com/nzlov/centrifugo/libcentrifugo/engine"
	"github.com/nzlov/centrifugo/libcentrifugo/logger"
	"github.com/nzlov/centrifugo/libcentrifugo/node"
	"github.com/nzlov/centrifugo/libcentrifugo/plugin"
	"github.com/nzlov/centrifugo/libcentrifugo/proto"

	"github.com/nzlov/go-cache"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

func init() {
	plugin.RegisterEngine("mgo", Plugin)
	plugin.RegisterConfigurator("mgo", Configure)
}

// Configure is a Configurator function for Redis engine.
func Configure(setter config.Setter) error {

	setter.StringFlag("mgo_url", "", "mongodb://:@localhost:27017", "mgo url (Mgo engine)")
	setter.StringFlag("mgo_dupl", "", "clone", "mgo dupl (Mgo engine)")
	setter.StringFlag("mgo_db", "", "centrifugo", "mgo db (Mgo engine)")

	bindFlags := []string{
		"mgo_url", "mgo_dupl", "mgo_db",
	}
	for _, flag := range bindFlags {
		setter.BindFlag(flag, flag)
	}

	bindEnvs := []string{"mgo_url", "mgo_dupl", "mgo_db"}
	for _, env := range bindEnvs {
		setter.BindEnv(env)
	}

	return nil
}

type MgoEngine struct {
	session     *mgo.Session
	sessionDupl func() *mgo.Session
	node        *node.Node
	presenceHub *presenceHub
	config      *Config
	expireCache *cache.Cache
}

// Plugin returns new memory engine.
func Plugin(n *node.Node, c config.Getter) (engine.Engine, error) {
	return New(n, &Config{
		URL:  c.GetString("mgo_url"),
		Dupl: c.GetString("mgo_dupl"),
		DB:   c.GetString("mgo_db"),
	})
}

type Config struct {
	URL  string
	Dupl string
	DB   string
}

// New initializes Memory Engine.
func New(n *node.Node, conf *Config) (*MgoEngine, error) {
	e := &MgoEngine{
		node:        n,
		presenceHub: newPresenceHub(),
		config:      conf,
		expireCache: cache.New(time.Minute, 2*time.Minute),
	}
	return e, nil
}

// Name returns a name of engine.
func (e *MgoEngine) Name() string {
	return "In MongoDB"
}

// Run runs memory engine - we do not have any logic here as Memory Engine ready to work
// just after initialization.
func (e *MgoEngine) Run() error {
	var err error
	e.session, err = mgo.Dial(e.config.URL)
	if err != nil {
		return err
	}
	switch e.config.Dupl {
	case "copy":
		e.sessionDupl = e.session.Copy
	case "new":
		e.sessionDupl = e.session.New
	default:
		e.sessionDupl = e.session.Clone

	}
	return nil
}

// Shutdown shuts down engine.
func (e *MgoEngine) Shutdown() error {
	return errors.New("Shutdown not implemented")
}

type presenceHub struct {
	sync.RWMutex
	presence map[string]map[string]proto.ClientInfo
}

func newPresenceHub() *presenceHub {
	return &presenceHub{
		presence: make(map[string]map[string]proto.ClientInfo),
	}
}

func (h *presenceHub) add(ch string, uid string, info proto.ClientInfo) error {
	h.Lock()
	defer h.Unlock()

	_, ok := h.presence[ch]
	if !ok {
		h.presence[ch] = make(map[string]proto.ClientInfo)
	}
	h.presence[ch][uid] = info
	return nil
}

func (h *presenceHub) remove(ch string, uid string) error {
	h.Lock()
	defer h.Unlock()

	if _, ok := h.presence[ch]; !ok {
		return nil
	}
	if _, ok := h.presence[ch][uid]; !ok {
		return nil
	}

	delete(h.presence[ch], uid)

	// clean up map if needed
	if len(h.presence[ch]) == 0 {
		delete(h.presence, ch)
	}

	return nil
}

func (h *presenceHub) get(ch string) (map[string]proto.ClientInfo, error) {
	h.RLock()
	defer h.RUnlock()

	presence, ok := h.presence[ch]
	if !ok {
		// return empty map
		return map[string]proto.ClientInfo{}, nil
	}

	var data map[string]proto.ClientInfo
	data = make(map[string]proto.ClientInfo, len(presence))
	for k, v := range presence {
		data[k] = v
	}
	return data, nil
}

// PublishMessage adds message into history hub and calls node ClientMsg method to handle message.
// We don't have any PUB/SUB here as Memory Engine is single node only.
func (e *MgoEngine) PublishMessage(message *proto.Message, opts *channel.Options) <-chan error {
	logger.DEBUG.Println("Engine Mgo:PublishMessage:", message)
	session := e.sessionDupl()
	defer session.Close()

	ch := message.Channel

	chs := strings.Split(ch, ":")
	tb := "default"
	if len(chs) >= 2 {
		tb = chs[0]
	}
	err := session.DB(e.config.DB).C(tb).Insert(bson.M{
		"uid":     message.UID,
		"channel": message.Channel,
		"client":  message.Client,
		"data":    message.Data,
		"info":    message.Info,
		"read":    message.Read,
		"addtime": message.Timestamp,
	})
	eChan := make(chan error, 1)
	if err != nil {
		logger.ERROR.Println("Engine Mgo:Publish Message Insert Has Error:", err)
		eChan <- err
		return eChan
	}

	eChan <- e.node.ClientMsg(message)
	return eChan
}

// PublishJoin - see Engine interface description.
func (e *MgoEngine) PublishJoin(message *proto.JoinMessage, opts *channel.Options) <-chan error {
	eChan := make(chan error, 1)
	eChan <- e.node.JoinMsg(message)
	return eChan
}

// PublishLeave - see Engine interface description.
func (e *MgoEngine) PublishLeave(message *proto.LeaveMessage, opts *channel.Options) <-chan error {
	eChan := make(chan error, 1)
	eChan <- e.node.LeaveMsg(message)
	return eChan
}

// PublishControl - see Engine interface description.
func (e *MgoEngine) PublishControl(message *proto.ControlMessage) <-chan error {
	eChan := make(chan error, 1)
	eChan <- e.node.ControlMsg(message)
	return eChan
}

// PublishAdmin - see Engine interface description.
func (e *MgoEngine) PublishAdmin(message *proto.AdminMessage) <-chan error {
	eChan := make(chan error, 1)
	eChan <- e.node.AdminMsg(message)
	return eChan
}

// Subscribe is noop here.
func (e *MgoEngine) Subscribe(ch string) error {
	return nil
}

// Unsubscribe node from channel.
// In case of memory engine its only job is to touch channel history for history lifetime period.
func (e *MgoEngine) Unsubscribe(ch string) error {
	return nil
}

// AddPresence adds client info into presence hub.
func (e *MgoEngine) AddPresence(ch string, uid string, info proto.ClientInfo, expire int) error {
	return e.presenceHub.add(ch, uid, info)
}

// RemovePresence removes client info from presence hub.
func (e *MgoEngine) RemovePresence(ch string, uid string) error {
	return e.presenceHub.remove(ch, uid)
}

// Presence extracts presence info from presence hub.
func (e *MgoEngine) Presence(ch string) (map[string]proto.ClientInfo, error) {
	return e.presenceHub.get(ch)
}

func (e *MgoEngine) ReadMessage(ch, msgid string) (bool, error) {
	logger.DEBUG.Println("Engine Mgo:ReadMessage:", ch, msgid)
	if ch == "" || msgid == "" {
		logger.ERROR.Println("Engine Mgo:ReadMessage:", ch, msgid)
		return false, proto.ErrInvalidMessage
	}

	key := ch + "-" + msgid
	if _, has := e.expireCache.Get(key); has {
		return true, nil
	}
	e.expireCache.Set(key, 1, cache.DefaultExpiration)

	session := e.sessionDupl()
	defer session.Close()
	chs := strings.Split(ch, ":")
	tb := "default"
	if len(chs) >= 2 {
		tb = chs[0]
	}

	err := session.DB(e.config.DB).C(tb).Update(bson.M{"uid": msgid}, bson.M{"$set": bson.M{"read": true}})
	if err != nil {
		logger.ERROR.Println("Engine Mgo:ReadMessage:has Error:", err)
		return false, err
	}
	resp := proto.NewClientReadResponse(proto.ReadBody{
		Channel: ch,
		MsgID:   msgid,
		Read:    true,
	})
	byteMessage, err := json.Marshal(resp)
	if err != nil {
		logger.ERROR.Println("Engine Mgo:ReadMessage:Marshal:", resp)
		return true, nil
	}
	e.node.ClientHub().Broadcast(ch, byteMessage)
	return true, nil
}

// History extracts history from history hub.
func (e *MgoEngine) History(ch string, skip, limit int) ([]proto.Message, int, error) {
	logger.DEBUG.Println("Engine Mgo:History:", ch, skip, limit)
	if ch == "" {
		logger.ERROR.Println("Engine Mgo:History:", ch, skip, limit)
		return []proto.Message{}, 0, proto.ErrInvalidMessage
	}
	session := e.sessionDupl()
	defer session.Close()

	chs := strings.Split(ch, ":")
	tb := "default"
	if len(chs) >= 2 {
		tb = chs[0]
	}

	total, err := session.DB(e.config.DB).C(tb).Count()
	if err != nil {
		logger.ERROR.Println("Engine Mgo:History:Count:has Error:", err)
		return []proto.Message{}, 0, proto.ErrInvalidMessage
	}
	msgs := []proto.Message{}

	sort := "addtime"
	if skip < 0 {
		sort = "-addtime"
		skip = -(skip) - 1
	}
	if limit < 0 {
		limit = total
	}

	logger.DEBUG.Println("Engine Mgo:History:", ch, sort, skip, limit)

	err = session.DB(e.config.DB).C(tb).Find(bson.M{}).Sort(sort).Skip(skip).Limit(limit).All(&msgs)
	if err != nil {
		return []proto.Message{}, 0, proto.ErrInvalidMessage
	}
	return msgs, total, nil
}

// Channels returns all channels node currently subscribed on.
func (e *MgoEngine) Channels() ([]string, error) {
	return e.node.ClientHub().Channels(), nil
}
