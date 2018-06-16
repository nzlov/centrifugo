package enginegorm

import (
	"database/sql/driver"
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

	"github.com/nzlov/go-cache"
	"github.com/tidwall/gjson"

	"github.com/lib/pq"
	"github.com/nzlov/gorm"
	_ "github.com/nzlov/gorm/dialects/mysql"
	_ "github.com/nzlov/gorm/dialects/postgres"
	_ "github.com/nzlov/gorm/dialects/sqlite"
)

func init() {
	plugin.RegisterEngine("gorm", Plugin)
	plugin.RegisterConfigurator("gorm", Configure)

}

// Configure is a Configurator function for Redis engine.
func Configure(setter config.Setter) error {

	setter.StringFlag("gorm_url", "", "mongodb://:@localhost:27017", "gorm url (gorm engine)")
	setter.StringFlag("gorm_dialects", "", "postgres", "gorm dialects (gorm engine)")

	setter.StringFlag("gorm_mode", "", "dev", "gorm run mode (gorm engine)")
	setter.IntFlag("gorm_go", "", 10, "gorm run gorm goroutine num (gorm engine)")
	setter.StringFlag("gorm_redis", "", "shanshou.redis.host:6379", "gorm db redis(gorm engine)")

	setter.StringFlag("gorm_jpush_merchant_key", "", "931402db6613187d7a9383e3", "gorm db (gorm engine)")
	setter.StringFlag("gorm_jpush_merchant_secret", "", "45eef5afcc117a5ac476d5a8", "gorm db (gorm engine)")
	setter.StringFlag("gorm_jpush_consumer_key", "", "6f14b16a0b966bf90f3efd92", "gorm db (gorm engine)")
	setter.StringFlag("gorm_jpush_consumer_secret", "", "65ceef7409918f5d7ca44a58", "gorm db (gorm engine)")

	setter.StringFlag("gorm_jpush_url", "", "https://api.jpush.cn", "gorm db (gorm engine)")
	setter.StringFlag("gorm_jpush_device", "", "https://device.jpush.cn", "gorm db (gorm engine)")
	setter.StringFlag("gorm_jpush_report", "", "https://report.jpush.cn", "gorm db (gorm engine)")

	bindFlags := []string{
		"gorm_url",
		"gorm_dialects",
		"gorm_mode",
		"gorm_go",
		"gorm_jpush_merchant_key",
		"gorm_jpush_merchant_secret",
		"gorm_jpush_consumer_key",
		"gorm_jpush_consumer_secret",
		"gorm_jpush_url",
		"gorm_jpush_device",
		"gorm_jpush_report",
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

type DBData struct {
	ID uint `gorm:"primary_key"`

	UID       string            `json:"uid"`
	Channel   string            `json:"channel"`
	Client    string            `json:"client"`
	Data      raw.Raw           `json:"data"  gorm:"type:jsonb"`
	Info      *proto.ClientInfo `json:"info" gorm:"type:jsonb"`
	Read      bool              `json:"read"`
	Timestamp int64             `json:"timestamp"`

	Appkey pq.StringArray `json:"-" gorm:"type:varchar(125)[]"`
	Users  pq.StringArray `json:"-" gorm:"type:varchar(125)[]"`
	NUsers pq.StringArray `json:"-" gorm:"type:varchar(125)[]"`
}

func (d DBData) TableName() string {
	chs := strings.Split(d.Channel, ":")
	tb := "defaults"
	if len(chs) == 2 {
		tb = chs[0]
	}
	return "centrifugo_" + tb
}

type Persence struct {
	ID uint `gorm:"primary_key"`

	User      string `gorm:"user"`
	Channel   string `gorm:"channel"`
	ChannelID string `gorm:"channelid"`
	Online    bool   `gorm:"online"`
}

func (Persence) TableName() string {
	return "centrifugo_persence"
}

type DataBakup struct {
	data raw.Raw
}

func (j DataBakup) Value() (driver.Value, error) {
	return []byte(j.data), nil
}

// Scan scan value into Jsonb
func (j *DataBakup) Scan(value interface{}) error {
	j.data = []byte(value.(string))
	return nil
}

type Engine struct {
	db          *gorm.DB
	node        *node.Node
	presenceHub *presenceHub
	config      *Config
	expireCache *cache.Cache

	messageChan chan *message
}

// Plugin returns new memory engine.
func Plugin(n *node.Node, c config.Getter) (engine.Engine, error) {
	return New(n, &Config{
		URL:                 c.GetString("gorm_url"),
		Mode:                c.GetString("gorm_mode"),
		Go:                  c.GetInt("gorm_go"),
		Redis:               c.GetString("gorm_redis"),
		Dialects:            c.GetString("gorm_dialects"),
		JPushMerchantKey:    c.GetString("gorm_jpush_merchant_key"),
		JPushMerchantSecret: c.GetString("gorm_jpush_merchant_secret"),
		JPushConsumerKey:    c.GetString("gorm_jpush_consumer_key"),
		JPushConsumerSecret: c.GetString("gorm_jpush_consumer_secret"),

		JpushUrl:       c.GetString("gorm_jpush_url"),
		JpushDeviceUrl: c.GetString("gorm_jpush_device"),
		JpushReportUrl: c.GetString("gorm_jpush_report"),
	})
}

type Config struct {
	URL      string
	Redis    string
	Dialects string
	Mode     string
	Go       int

	JPushMerchantKey    string
	JPushMerchantSecret string
	JPushConsumerKey    string
	JPushConsumerSecret string
	JpushUrl            string
	JpushDeviceUrl      string
	JpushReportUrl      string
}

// New initializes Memory Engine.
func New(n *node.Node, conf *Config) (*Engine, error) {
	e := &Engine{
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
	models.InitDB(conf.URL)
	models.InitRedis(conf.Redis, 10)
	return e, nil
}

// Name returns a name of engine.
func (e *Engine) Name() string {
	return "In Grom with" + e.config.Dialects
}

// Run runs memory engine - we do not have any logic here as Memory Engine ready to work
// just after initialization.
func (e *Engine) Run() error {
	var err error
	e.db, err = gorm.Open(e.config.Dialects, e.config.URL)
	if err != nil {
		logger.ERROR.Println("[GORM] Engine opens has error:", err.Error())
		return err
	}
	if e.config.Mode != "prod" {
		e.db.LogMode(true)
	}
	e.db.AutoMigrate(new(Persence))

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

	e.messageChan = make(chan *message, e.config.Go*10)
	for i := 0; i < e.config.Go; i++ {
		go e.Save()
	}

	return nil
}

// Shutdown shuts down engine.
func (e *Engine) Shutdown() error {
	e.expireCache.OnEvicted(nil)

	return e.db.Close()
}

func (e *Engine) Forbidden(raw.Raw) bool {
	return false
}
func (e *Engine) Permission(eid, permission string) bool {
	einfo := models.Employee{
		EmployeesID: eid,
	}
	err := models.EmployeeGet(e.db.New(), &einfo)
	if err != nil {
		logger.ERROR.Println("[GORM] Engine Permission Get Employees has error:", err.Error())
		return false
	}
	for _, v := range einfo.Role.Permissions {
		if v == permission {
			return true
		}
	}
	return false
}
func (e *Engine) save(db *gorm.DB, appkey, users, nusers string, message *proto.Message) error {

	data := &DBData{
		UID:       message.UID,
		Channel:   message.Channel,
		Client:    message.Client,
		Data:      message.Data,
		Info:      message.Info,
		Read:      message.Read,
		Timestamp: message.Timestamp,
	}

	oappkeys := strings.Split(appkey, ",")
	appkeys := []string{}
	for _, v := range oappkeys {
		if v != "" {
			appkeys = append(appkeys, v)
		}
	}
	if len(appkeys) > 0 {
		data.Appkey = pq.StringArray(appkeys)
	}
	ouserss := strings.Split(users, ",")
	userss := []string{}
	for _, v := range ouserss {
		if v != "" {
			userss = append(userss, v)
		}
	}
	if len(userss) > 0 {
		data.Users = pq.StringArray(userss)
	}
	onuserss := strings.Split(nusers, ",")
	nuserss := []string{}
	for _, v := range onuserss {
		if v != "" {
			nuserss = append(nuserss, v)
		}
	}
	if len(nuserss) > 0 {
		data.NUsers = pq.StringArray(nuserss)
	}

	db.AutoMigrate(data)
	err := db.Create(data).Error
	if err != nil {
		logger.ERROR.Println("[GORM] Engine save has error:", err.Error())
		return err
	}
	return nil
}
func (e *Engine) Save() {
	defer func() {
		if err := recover(); err != nil {
			logger.ERROR.Println(err)
			go e.Save()
		}
	}()
	for m := range e.messageChan {
		db := e.db.New()

		message := m.message
		errChan := m.errChan

		if err := e.save(db, m.appkey, m.users, m.nusers, message); err != nil {
			errChan <- err
			logger.ERROR.Println(err)
		}
		errChan <- e.node.ClientMsg(message, m.appkey, m.users, m.nusers)

		gjsons := gjson.ParseBytes([]byte(message.Data))
		t := gjsons.Get("type")
		logger.DEBUG.Println("[GORM] Engine:PublishMessage:", t)
		if t.Exists() {
			switch t.String() {
			case "chat":
				chat := models.CentrifugoMessageChat{}
				err := json.Unmarshal([]byte(message.Data), &chat)
				if err != nil {
					logger.ERROR.Println("[GORM] Engine:PublishMessage chat type Message:", string(message.Data), err.Error())
				}
				ch := strings.Split(message.Channel, ":")
				if len(ch) == 2 {
					switch ch[0] {
					case "users":
						//发给店铺
						newMessage := proto.NewMessage(chat.To, []byte(message.Data), message.Client, message.Info)
						if err := e.save(db, models.CENTRIFUGOAPPKEY_MERCHANT, "", "", newMessage); err != nil {
							logger.ERROR.Println("[GORM] Engine:PublishMessage send users save has error:", string(message.Data), err.Error())
						}
						if err = e.node.ClientMsg(newMessage, models.CENTRIFUGOAPPKEY_MERCHANT, "", ""); err != nil {
							logger.DEBUG.Println("[GORM] Engine:PublishMessage:send users Broadcast has error:", err.Error())
						}
						if err = models.CentrifugoOfflineJPush(db, "a_merchan", strings.Split(chat.To, ":")[1], newMessage.UID, chat.To, chat); err != nil {
							logger.DEBUG.Println("[GORM] Engine:PublishMessage:send users CentrifugoOfflineJPush has error:", err.Error())
						}
					case "shops":
						//发给顾客端
						chat.From = message.Channel
						shopinfo := models.Shop{
							ShopsID: ch[1],
						}
						if err = models.ShopGet(db, &shopinfo); err != nil {
							logger.ERROR.Println("[GORM] Engine:PublishMessage send shops find shop has error:", string(message.Data), err.Error())
						}
						chat.Name = shopinfo.ShopName
						data, _ := json.Marshal(&chat)
						newMessage := proto.NewMessage(chat.To, data, message.Client, message.Info)
						if err := e.save(db, models.CENTRIFUGOAPPKEY_CONSUME, "", "", newMessage); err != nil {
							logger.ERROR.Println("[GORM] Engine:PublishMessage send shops save has error:", string(message.Data), err.Error())
						}
						if err = e.node.ClientMsg(newMessage, models.CENTRIFUGOAPPKEY_CONSUME, "", ""); err != nil {
							logger.DEBUG.Println("[GORM] Engine:PublishMessage:send shops Broadcast has error:", err.Error())
						}
						if err = models.CentrifugoOfflineJPush(db, "a_consume", "", newMessage.UID, chat.To, chat); err != nil {
							logger.DEBUG.Println("[GORM] Engine:PublishMessage:send shops CentrifugoOfflineJPush has error:", err.Error())
						}
					}
				}
			}
		}
	}
}

// PublishMessage adds message into history hub and calls node ClientMsg method to handle message.
// We don't have any PUB/SUB here as Memory Engine is single node only.
func (e *Engine) PublishMessage(message *proto.Message, appkey, users, nusers string, opts *channel.Options) <-chan error {
	logger.DEBUG.Println("[GORM] Engine:PublishMessage:", message)
	eChan := make(chan error)

	e.publishMessage(message, appkey, users, nusers, eChan)
	logger.DEBUG.Println("[GORM] Engine:PublishMessage OK.")
	return eChan
}

func (e *Engine) publishMessage(m *proto.Message, appkey, users, nusers string, eChan chan error) {
	e.messageChan <- &message{
		message: m,
		errChan: eChan,
		appkey:  appkey,
		users:   users,
		nusers:  nusers,
	}
}

// PublishJoin - see Engine interface description.
func (e *Engine) PublishJoin(message *proto.JoinMessage, opts *channel.Options) <-chan error {
	eChan := make(chan error, 1)
	eChan <- e.node.JoinMsg(message)
	return eChan
}

// PublishLeave - see Engine interface description.
func (e *Engine) PublishLeave(message *proto.LeaveMessage, opts *channel.Options) <-chan error {
	eChan := make(chan error, 1)
	eChan <- e.node.LeaveMsg(message)
	return eChan
}

// PublishControl - see Engine interface description.
func (e *Engine) PublishControl(message *proto.ControlMessage) <-chan error {
	eChan := make(chan error, 1)
	eChan <- e.node.ControlMsg(message)
	return eChan
}

// PublishAdmin - see Engine interface description.
func (e *Engine) PublishAdmin(message *proto.AdminMessage) <-chan error {
	eChan := make(chan error, 1)
	eChan <- e.node.AdminMsg(message)
	return eChan
}

// Subscribe is noop here.
func (e *Engine) Subscribe(ch string) error {
	return nil
}

// Unsubscribe node from channel.
// In case of memory engine its only job is to touch channel history for history lifetime period.
func (e *Engine) Unsubscribe(ch string) error {
	return nil
}

// AddPresence adds client info into presence hub.
func (e *Engine) AddPresence(ch string, uid string, info proto.ClientInfo, expire int) error {
	//	logger.DEBUG.Println("Presence:Add :", ch, uid, info)
	chs := strings.Split(ch, ":")
	if len(chs) == 2 {
		key := "presence_" + ch + "_" + info.User
		if _, has := e.expireCache.Get(key); has {
			e.expireCache.Set(key, 1, cache.DefaultExpiration)
			return nil
		}
		e.expireCache.Set(key, 1, cache.DefaultExpiration)

		db := e.db.New()
		presence := Persence{
			User:    info.User,
			Channel: chs[0],
		}
		db.Where(&presence).Find(&presence)
		presence.ChannelID = chs[1]
		presence.Online = true
		err := db.Save(&presence).Error
		if err != nil {
			logger.ERROR.Println("[GORM] Engine AddPresence save presence has error:", err.Error())
		}
	}
	return e.presenceHub.add(ch, uid, info)
}

// RemovePresence removes client info from presence hub.
func (e *Engine) RemovePresence(ch, uid, user string) error {
	//	logger.DEBUG.Println("Presence:Remove :", ch, uid, user)
	chs := strings.Split(ch, ":")
	if len(chs) == 2 {
		e.expireCache.Delete("presence_" + ch + "_" + user)

		db := e.db.New()

		presence := Persence{
			User:    user,
			Channel: chs[0],
		}

		err := db.Where(&presence).Find(&presence).Error
		if err == nil {
			presence.Online = false
			err = db.Save(&presence).Error
			if err != nil {
				logger.ERROR.Println("[GORM] Engine RemovePresence save presence has error:", err.Error())
			}
		}
	}
	return e.presenceHub.remove(ch, uid)
}

// Presence extracts presence info from presence hub.
func (e *Engine) Presence(ch string) (map[string]proto.ClientInfo, error) {
	return e.presenceHub.get(ch)
}

func (e *Engine) ReadMessage(ch, msgid, uid string) (bool, error) {
	if ch == "" || msgid == "" {
		logger.ERROR.Println("[GORM] Engine:ReadMessage:", ch, msgid)
		return false, proto.ErrInvalidMessage
	}

	key := ch + "-" + msgid
	if _, has := e.expireCache.Get(key); has {
		return true, nil
	}
	e.expireCache.Set(key, 1, cache.DefaultExpiration)

	logger.DEBUG.Println("[GORM] Engine:ReadMessage:", ch, uid, msgid)
	db := e.db.New()
	data := DBData{
		Channel: ch,
		UID:     msgid,
	}
	err := db.Where(&data).Find(&data).Error
	if err != nil {
		logger.ERROR.Println("[GORM] Engine:ReadMessage:Find data has Error:", err)
		return false, err
	}
	data.Read = true
	err = db.Save(&data).Error
	if err != nil {
		logger.ERROR.Println("[GORM] Engine:ReadMessage:Save data has Error:", err)
		return false, err
	}
	resp := proto.NewClientReadResponse(proto.ReadBody{
		Channel: ch,
		MsgID:   msgid,
		Read:    true,
	})
	byteMessage, err := json.Marshal(resp)
	if err != nil {
		logger.ERROR.Println("[GORM] Engine:ReadMessage:Marshal:", resp)
		return true, nil
	}
	e.node.ClientHub().Broadcast(ch, "", byteMessage, "", uid)
	return true, nil
}

// History extracts history from history hub.
func (e *Engine) History(ch, appkey, client string, skip, limit int) ([]proto.Message, int, error) {
	logger.DEBUG.Println("[GORM] Engine:History:", ch, skip, limit)
	if ch == "" {
		logger.ERROR.Println("[GORM] Engine:History:", ch, skip, limit)
		return []proto.Message{}, 0, proto.ErrInvalidMessage
	}
	db := e.db.New()

	data := DBData{
		Channel: ch,
	}
	db.AutoMigrate(data)

	t := db.Model(&data).Where(`
		channel = ? 
		and 
		timestamp >= ? 
		and 
		( 
			appkey is null or ? = any(appkey) 
		) 
		and 
		( 
			users is null 
			or
			(
				? = any(users)
				and
				(
					n_users is null or ? = any(n_users)
				)
			) 
		) 
		and
		( 
			n_users is null or ? = any(n_users)
		)`, ch, time.Now().Add(-time.Hour*168).UnixNano(), appkey, client, client, client)

	// query := bson.M{
	// 	"channel": ch,
	// 	"$and": []bson.M{
	// 		{
	// 			"$or": []bson.M{
	// 				{
	// 					"appkey": bson.M{"$exists": false},
	// 				}, {
	// 					"appkey": appkey,
	// 				},
	// 			},
	// 		},
	// 		{
	// 			"$or": []bson.M{
	// 				{
	// 					"users": bson.M{"$exists": false},
	// 				}, {
	// 					"users": client,
	// 					"$or": []bson.M{
	// 						{
	// 							"nusers": bson.M{"$exists": false},
	// 						},
	// 						{
	// 							"nusers": bson.M{"$ne": client},
	// 						},
	// 					},
	// 				},
	// 			},
	// 		},
	// 	},
	// 	"$or": []bson.M{
	// 		{
	// 			"nusers": bson.M{"$exists": false},
	// 		},
	// 		{
	// 			"nusers": bson.M{"$ne": client},
	// 		},
	// 	},
	// 	"timestamp": bson.M{
	// 		"$gte": time.Now().Add(-time.Hour * 168).UnixNano(),
	// 	},
	// }

	//jsondata, _ := json.MarshalIndent(query, "", "  ")
	//logger.DEBUG.Printf("Query:%+v\n", string(jsondata))

	total := 0
	err := t.Count(&total).Error
	if err != nil {
		logger.ERROR.Println("[GORM] Engine:History:Count:has Error:", err.Error())
		return []proto.Message{}, 0, proto.ErrInvalidMessage
	}
	msgs := []proto.Message{}

	sort := "timestamp"
	if skip < 0 {
		sort = "timestamp desc"
		skip = -(skip) - 1
	}
	if limit < 0 {
		limit = total
	}

	logger.DEBUG.Println("[GORM] Engine:History:", ch, sort, skip, limit)

	err = t.Table(data.TableName()).Order(sort).Offset(sort).Limit(limit).Find(&msgs).Error
	if err != nil {
		logger.ERROR.Println("[GORM] Engine:History:Find:has Error:", err.Error())
		return []proto.Message{}, 0, proto.ErrInvalidMessage
	}
	logger.DEBUG.Println("[GORM] Engine:History:Has:", total, len(msgs))
	return msgs, total, nil
}

// Channels returns all channels node currently subscribed on.
func (e *Engine) Channels() ([]string, error) {
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
