package enginemgo

import (
	"encoding/json"
	"strings"
	"sync"
	"time"

	"kabao/app/models"

	"github.com/nzlov/centrifugo/libcentrifugo/channel"
	"github.com/nzlov/centrifugo/libcentrifugo/config"
	"github.com/nzlov/centrifugo/libcentrifugo/engine"
	"github.com/nzlov/centrifugo/libcentrifugo/logger"
	"github.com/nzlov/centrifugo/libcentrifugo/node"
	"github.com/nzlov/centrifugo/libcentrifugo/plugin"
	"github.com/nzlov/centrifugo/libcentrifugo/proto"
	"github.com/nzlov/centrifugo/libcentrifugo/raw"
	"github.com/tidwall/gjson"

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

	setter.StringFlag("mgo_mode", "", "dev", "mgo run mode (Mgo engine)")
	setter.IntFlag("mgo_go", "", 10, "mgo run mgo goroutine num (Mgo engine)")

	setter.StringFlag("mgo_jpush_merchant_key", "", "931402db6613187d7a9383e3", "mgo db (Mgo engine)")
	setter.StringFlag("mgo_jpush_merchant_secret", "", "45eef5afcc117a5ac476d5a8", "mgo db (Mgo engine)")
	setter.StringFlag("mgo_jpush_consumer_key", "", "6f14b16a0b966bf90f3efd92", "mgo db (Mgo engine)")
	setter.StringFlag("mgo_jpush_consumer_secret", "", "65ceef7409918f5d7ca44a58", "mgo db (Mgo engine)")

	setter.StringFlag("mgo_jpush_url", "", "https://api.jpush.cn", "mgo db (Mgo engine)")
	setter.StringFlag("mgo_jpush_device", "", "https://device.jpush.cn", "mgo db (Mgo engine)")
	setter.StringFlag("mgo_jpush_report", "", "https://report.jpush.cn", "mgo db (Mgo engine)")

	bindFlags := []string{
		"mgo_url",
		"mgo_dupl",
		"mgo_db",
		"mgo_mode",
		"mgo_go",
		"mgo_jpush_merchant_key",
		"mgo_jpush_merchant_secret",
		"mgo_jpush_consumer_key",
		"mgo_jpush_consumer_secret",
		"mgo_jpush_url",
		"mgo_jpush_device",
		"mgo_jpush_report",
	}
	for _, flag := range bindFlags {
		setter.BindEnv(flag)
		setter.BindFlag(flag, flag)
	}

	return nil
}

type message struct {
	message *proto.Message
	appkey  string
	users   string
	nusers  string
	errChan chan error
}
type MgoEngine struct {
	session     *mgo.Session
	sessionDupl func() *mgo.Session
	node        *node.Node
	presenceHub *presenceHub
	config      *Config
	expireCache *cache.Cache

	mgoMessageChan chan *message
}

// Plugin returns new memory engine.
func Plugin(n *node.Node, c config.Getter) (engine.Engine, error) {
	return New(n, &Config{
		URL:                 c.GetString("mgo_url"),
		Dupl:                c.GetString("mgo_dupl"),
		DB:                  c.GetString("mgo_db"),
		Mode:                c.GetString("mgo_mode"),
		Go:                  c.GetInt("mgo_go"),
		JPushMerchantKey:    c.GetString("mgo_jpush_merchant_key"),
		JPushMerchantSecret: c.GetString("mgo_jpush_merchant_secret"),
		JPushConsumerKey:    c.GetString("mgo_jpush_consumer_key"),
		JPushConsumerSecret: c.GetString("mgo_jpush_consumer_secret"),

		JpushUrl:       c.GetString("mgo_jpush_url"),
		JpushDeviceUrl: c.GetString("mgo_jpush_device"),
		JpushReportUrl: c.GetString("mgo_jpush_report"),
	})
}

type Config struct {
	URL   string
	Dupl  string
	DB    string
	Redis string
	Mode  string
	Go    int

	JPushMerchantKey    string
	JPushMerchantSecret string
	JPushConsumerKey    string
	JPushConsumerSecret string
	JpushUrl            string
	JpushDeviceUrl      string
	JpushReportUrl      string
}

// New initializes Memory Engine.
func New(n *node.Node, conf *Config) (*MgoEngine, error) {
	e := &MgoEngine{
		node:        n,
		presenceHub: newPresenceHub(),
		config:      conf,
		expireCache: cache.New(time.Minute, 2*time.Minute),
	}
	switch conf.Mode {
	case "prod":
		models.SysModelDev = false
	default:
		models.SysModelDev = true
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
	e.expireCache.OnEvicted(func(k string, v interface{}) {
		logger.DEBUG.Println("Mgo Read Expire Cache:Evicted:", k)
	})

	models.JpushUrl = e.config.JpushUrl
	models.JpushDeviceUrl = e.config.JpushDeviceUrl
	models.JpushReportUrl = e.config.JpushReportUrl

	models.JPUSHKEYS["merchant"] = models.JPUSHKEY{
		Key:    e.config.JPushMerchantKey,
		Sercet: e.config.JPushMerchantSecret,
	}
	models.JPUSHKEYS["consume"] = models.JPUSHKEY{
		Key:    e.config.JPushConsumerKey,
		Sercet: e.config.JPushConsumerSecret,
	}

	e.mgoMessageChan = make(chan *message, e.config.Go*10)
	for i := 0; i < e.config.Go; i++ {
		go e.mgoSave()
	}

	return nil
}

// Shutdown shuts down engine.
func (e *MgoEngine) Shutdown() error {
	e.expireCache.OnEvicted(nil)

	return nil
}

func (e *MgoEngine) Forbidden(raw.Raw) bool {
	return false
}
func (e *MgoEngine) Permission(eid, permission string) bool {
	session := e.sessionDupl()
	defer session.Close()
	einfo := models.GetEmployeeInfo(session, bson.M{"employeeid": eid, "permissions": bson.M{"$in": []string{permission}}})
	if einfo.EmployeeId == eid {
		return true
	}
	return false
}
func (e *MgoEngine) mgosave(session *mgo.Session, appkey, users, nusers string, message *proto.Message) error {
	ch := message.Channel

	chs := strings.Split(ch, ":")
	tb := "default"
	if len(chs) >= 2 {
		tb = chs[0]
	}

	databakup := map[string]interface{}{}
	err := json.Unmarshal([]byte(message.Data), &databakup)
	if err != nil {
		logger.ERROR.Println("mgosave databakup has error :", err)
	}
	b := bson.M{
		"uid":       message.UID,
		"channel":   message.Channel,
		"client":    message.Client,
		"data":      message.Data,
		"info":      message.Info,
		"read":      message.Read,
		"timestamp": message.Timestamp,
		"addtime":   time.Now(),
		"databakup": databakup,
	}
	if len(appkey) > 0 {
		b["appkey"] = strings.Split(appkey, ",")
	}
	userss := strings.Split(users, ",")
	if len(userss) > 0 {
		b["users"] = userss
	}
	nuserss := strings.Split(nusers, ",")
	if len(nuserss) > 0 {
		b["nusers"] = nuserss
	}
	err = session.DB(e.config.DB).C(tb).Insert(b)
	if err != nil {
		logger.ERROR.Println("Engine Mgo:Publish Message Insert Has Error:", err)
		return err
	}
	return nil
}
func (e *MgoEngine) mgoSave() {
	session := e.sessionDupl()
	defer session.Close()
	for m := range e.mgoMessageChan {
		defer func() {
			if err := recover(); err != nil {
				logger.ERROR.Println(err)
			}
		}()
		message := m.message
		errChan := m.errChan

		if err := e.mgosave(session, m.appkey, m.users, m.nusers, message); err != nil {
			errChan <- err
			continue
		}
		errChan <- e.node.ClientMsg(message, m.appkey, m.users, m.nusers)

		gjsons := gjson.ParseBytes([]byte(message.Data))
		t := gjsons.Get("type")
		logger.DEBUG.Println("Engine Mgo:PublishMessage:", t)
		if t.Exists() {
			switch t.String() {
			case "chat":
				chat := models.CentrifugoMessageChat{}
				err := json.Unmarshal([]byte(message.Data), &chat)
				if err != nil {
					logger.ERROR.Println("PublishMessage chat type Message:", string(message.Data), err)
					continue
				}
				ch := strings.Split(message.Channel, ":")
				if len(ch) == 2 {
					switch ch[0] {
					case "users":
						//发给店铺
						newMessage := proto.NewMessage(chat.To, []byte(message.Data), message.Client, message.Info)
						if err := e.mgosave(session, "", "", "", newMessage); err != nil {
							continue
						}
						err = e.node.ClientMsg(newMessage, "", "", "")
						logger.DEBUG.Println("Engine Mgo:PublishMessage:newMessage:publishMessage:", err)
						err = models.CentrifugoOfflineJPush(session, "a_merchant", strings.Split(chat.To, ":")[1], newMessage.UID, chat.To, chat)
						logger.DEBUG.Println("Engine Mgo:PublishMessage:newMessage:", err)
					case "shops":
						//发给顾客端
						chat.From = message.Channel
						shopinfo := models.GetShopInfo(session, bson.M{"shopid": ch[1]})
						chat.Name = shopinfo.ShopName
						data, _ := json.Marshal(&chat)
						newMessage := proto.NewMessage(chat.To, data, message.Client, message.Info)
						if err := e.mgosave(session, "", "", "", newMessage); err != nil {
							continue
						}
						err = e.node.ClientMsg(newMessage, "", "", "")
						logger.DEBUG.Println("Engine Mgo:PublishMessage:newMessage:publishMessage:", err)
						err = models.CentrifugoOfflineJPush(session, "a_consume", "", newMessage.UID, chat.To, chat)
						logger.DEBUG.Println("Engine Mgo:PublishMessage:CentrifugoOfflineJPush:", err)
					}
				}
			}
		}
	}
}

// PublishMessage adds message into history hub and calls node ClientMsg method to handle message.
// We don't have any PUB/SUB here as Memory Engine is single node only.
func (e *MgoEngine) PublishMessage(message *proto.Message, appkey, users, nusers string, opts *channel.Options) <-chan error {
	logger.DEBUG.Println("Engine Mgo:PublishMessage:", message)
	eChan := make(chan error)

	e.publishMessage(message, appkey, users, nusers, eChan)
	logger.DEBUG.Println("Engine Mgo:PublishMessage OK.")
	return eChan
}

func (e *MgoEngine) publishMessage(m *proto.Message, appkey, users, nusers string, eChan chan error) {
	e.mgoMessageChan <- &message{
		message: m,
		errChan: eChan,
		appkey:  appkey,
		users:   users,
		nusers:  nusers,
	}
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
	//	logger.DEBUG.Println("Presence:Add :", ch, uid, info)
	chs := strings.Split(ch, ":")
	if len(chs) == 2 {
		key := "presence_" + ch + "_" + info.User
		if _, has := e.expireCache.Get(key); has {
			e.expireCache.Set(key, 1, cache.DefaultExpiration)
			return nil
		}
		e.expireCache.Set(key, 1, cache.DefaultExpiration)
		session := e.sessionDupl()
		defer session.Close()
		session.DB(e.config.DB).C("presence").Upsert(bson.M{"user": info.User, "channel": chs[0]}, bson.M{"$set": bson.M{"channelid": chs[1], "online": true}})
	}
	return e.presenceHub.add(ch, uid, info)
}

// RemovePresence removes client info from presence hub.
func (e *MgoEngine) RemovePresence(ch, uid, user string) error {
	//	logger.DEBUG.Println("Presence:Remove :", ch, uid, user)
	chs := strings.Split(ch, ":")
	if len(chs) == 2 {
		e.expireCache.Delete("presence_" + ch + "_" + user)
		session := e.sessionDupl()
		defer session.Close()
		session.DB(e.config.DB).C("presence").Upsert(bson.M{"user": user, "channel": chs[0]}, bson.M{"$set": bson.M{"online": false}})
	}
	return e.presenceHub.remove(ch, uid)
}

// Presence extracts presence info from presence hub.
func (e *MgoEngine) Presence(ch string) (map[string]proto.ClientInfo, error) {
	return e.presenceHub.get(ch)
}

func (e *MgoEngine) ReadMessage(ch, appkey, msgid, uid string) (bool, error) {
	if ch == "" || msgid == "" {
		logger.ERROR.Println("Engine Mgo:ReadMessage:", ch, msgid)
		return false, proto.ErrInvalidMessage
	}

	key := ch + "-" + msgid
	if _, has := e.expireCache.Get(key); has {
		return true, nil
	}
	e.expireCache.Set(key, 1, cache.DefaultExpiration)

	logger.DEBUG.Println("Engine Mgo:ReadMessage:", ch, uid, msgid)
	session := e.sessionDupl()
	defer session.Close()
	chs := strings.Split(ch, ":")
	tb := "default"
	if len(chs) >= 2 {
		tb = chs[0]
	}

	err := session.DB(e.config.DB).C(tb).Update(bson.M{"channel": ch, "uid": msgid}, bson.M{"$set": bson.M{"read": true}})
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
	e.node.ClientHub().Broadcast(ch, appkey, byteMessage, "", uid)
	return true, nil
}

// History extracts history from history hub.
func (e *MgoEngine) History(ch, appkey, client string, skip, limit int) ([]proto.Message, int, error) {
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
	query := bson.M{
		"channel": ch,
		"$and": []bson.M{
			{
				"$or": []bson.M{
					{
						"appkey": bson.M{"$exists": false},
					}, {
						"appkey": appkey,
					},
				},
			},
			{
				"$or": []bson.M{
					{
						"users": bson.M{"$exists": false},
					}, {
						"users": client,
						"$or": []bson.M{
							{
								"nusers": bson.M{"$exists": false},
							},
							{
								"nusers": bson.M{"$ne": client},
							},
						},
					},
				},
			},
		},
		"$or": []bson.M{
			{
				"nusers": bson.M{"$exists": false},
			},
			{
				"nusers": bson.M{"$ne": client},
			},
		},
		"timestamp": bson.M{
			"$gte": time.Now().Add(-time.Hour * 168).UnixNano(),
		},
	}

	logger.DEBUG.Printf("Query:%+v\n", query)
	total, err := session.DB(e.config.DB).C(tb).Find(query).Count()
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

	err = session.DB(e.config.DB).C(tb).Find(query).Sort(sort).Skip(skip).Limit(limit).All(&msgs)
	if err != nil {
		return []proto.Message{}, 0, proto.ErrInvalidMessage
	}
	logger.DEBUG.Println("Engine Mgo:History:Has:", total, len(msgs))
	return msgs, total, nil
}

// Channels returns all channels node currently subscribed on.
func (e *MgoEngine) Channels() ([]string, error) {
	return e.node.ClientHub().Channels(), nil
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
