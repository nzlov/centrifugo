// Package node is a real-time core for Centrifugo server.
package node

import (
	"encoding/json"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/nzlov/centrifugo/libcentrifugo/channel"
	"github.com/nzlov/centrifugo/libcentrifugo/conns"
	"github.com/nzlov/centrifugo/libcentrifugo/engine"
	"github.com/nzlov/centrifugo/libcentrifugo/logger"
	"github.com/nzlov/centrifugo/libcentrifugo/metrics"
	"github.com/nzlov/centrifugo/libcentrifugo/proto"
	"github.com/nzlov/centrifugo/libcentrifugo/raw"
	"github.com/satori/go.uuid"
)

// Node is a heart of Centrifugo – it internally manages client and admin hubs,
// maintains information about other Centrifugo nodes, keeps references to
// config, engine, metrics etc.
type Node struct {
	mu sync.RWMutex

	// version
	version string

	// unique id for this node.
	uid string

	// started is unix time of node start.
	started int64

	// hub to manage client connections.
	clients conns.ClientHub

	// hub to manage admin connections.
	admins conns.AdminHub

	// config for node.
	config *Config

	// engine to use - in memory or redis.
	engine engine.Engine

	nodes *nodeRegistry

	// shutdown is a flag which is only true when node is going to shut down.
	shutdown bool

	// shutdownCh is a channel which is closed when shutdown happens.
	shutdownCh chan struct{}

	// save metrics snapshot until next metrics interval.
	metricsSnapshot map[string]int64

	// protect access to metrics snapshot.
	metricsMu sync.RWMutex
}

// global metrics registry pointing to the same Registry plugin package uses.
var metricsRegistry *metrics.Registry

func init() {
	metricsRegistry = metrics.DefaultRegistry

	metricsRegistry.RegisterCounter("node_num_client_msg_published", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_join_msg_published", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_leave_msg_published", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_admin_msg_published", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_control_msg_published", metrics.NewCounter())

	metricsRegistry.RegisterCounter("node_num_client_msg_received", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_join_msg_received", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_leave_msg_received", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_admin_msg_received", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_control_msg_received", metrics.NewCounter())

	metricsRegistry.RegisterCounter("node_num_add_client_conn", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_remove_client_conn", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_add_client_sub", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_remove_client_sub", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_presence", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_add_presence", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_remove_presence", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_history", metrics.NewCounter())
	metricsRegistry.RegisterCounter("node_num_last_message_id", metrics.NewCounter())

	metricsRegistry.RegisterGauge("node_memory_sys", metrics.NewGauge())
	metricsRegistry.RegisterGauge("node_memory_heap_sys", metrics.NewGauge())
	metricsRegistry.RegisterGauge("node_memory_heap_alloc", metrics.NewGauge())
	metricsRegistry.RegisterGauge("node_memory_stack_inuse", metrics.NewGauge())

	metricsRegistry.RegisterGauge("node_cpu_usage", metrics.NewGauge())
	metricsRegistry.RegisterGauge("node_num_goroutine", metrics.NewGauge())
	metricsRegistry.RegisterGauge("node_num_clients", metrics.NewGauge())
	metricsRegistry.RegisterGauge("node_num_unique_clients", metrics.NewGauge())
	metricsRegistry.RegisterGauge("node_num_channels", metrics.NewGauge())
	metricsRegistry.RegisterGauge("node_uptime_seconds", metrics.NewGauge())
}

// VERSION of Centrifugo server node. Set on build stage.
var VERSION string

// New creates Node, the only required argument is config.
func New(c *Config) *Node {
	uid := uuid.NewV4().String()

	n := &Node{
		version:         VERSION,
		uid:             uid,
		nodes:           newNodeRegistry(uid),
		config:          c,
		clients:         conns.NewClientHub(),
		admins:          conns.NewAdminHub(),
		started:         time.Now().Unix(),
		metricsSnapshot: make(map[string]int64),
		shutdownCh:      make(chan struct{}),
	}

	// Create initial snapshot with empty values.
	n.metricsMu.Lock()
	n.metricsSnapshot = n.getSnapshotMetrics()
	n.metricsMu.Unlock()

	return n
}

// Config returns a copy of node Config.
func (n *Node) Config() Config {
	n.mu.RLock()
	c := *n.config
	n.mu.RUnlock()
	return c
}

// SetConfig binds config to node.
func (n *Node) SetConfig(c *Config) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.config = c
}

// Version returns version of node.
func (n *Node) Version() string {
	return n.version
}

// Reload node.
func (n *Node) Reload(c *Config) error {
	if err := c.Validate(); err != nil {
		return err
	}
	n.SetConfig(c)
	return nil
}

// Engine returns node's Engine.
func (n *Node) Engine() engine.Engine {
	return n.engine
}

// ClientHub returns node's client hub.
func (n *Node) ClientHub() conns.ClientHub {
	return n.clients
}

// AdminHub returns node's admin hub.
func (n *Node) AdminHub() conns.AdminHub {
	return n.admins
}

// NotifyShutdown returns a channel which will be closed on node shutdown.
func (n *Node) NotifyShutdown() chan struct{} {
	return n.shutdownCh
}

// Run performs all startup actions. At moment must be called once on start
// after engine and structure set.
func (n *Node) Run(e engine.Engine) error {
	n.mu.Lock()
	n.engine = e
	n.mu.Unlock()

	if err := n.engine.Run(); err != nil {
		return err
	}

	err := n.pubPing()
	if err != nil {
		logger.CRITICAL.Println(err)
	}
	go n.sendNodePingMsg()
	go n.cleanNodeInfo()
	go n.updateMetrics()

	return nil
}

// Shutdown sets shutdown flag and does various clean ups.
func (n *Node) Shutdown() error {
	n.mu.Lock()
	if n.shutdown {
		n.mu.Unlock()
		return nil
	}
	n.shutdown = true
	close(n.shutdownCh)
	n.mu.Unlock()
	return n.clients.Shutdown()
}

func (n *Node) updateMetricsOnce() {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	metricsRegistry.Gauges.Set("node_memory_sys", int64(mem.Sys))
	metricsRegistry.Gauges.Set("node_memory_heap_sys", int64(mem.HeapSys))
	metricsRegistry.Gauges.Set("node_memory_heap_alloc", int64(mem.HeapAlloc))
	metricsRegistry.Gauges.Set("node_memory_stack_inuse", int64(mem.StackInuse))
	if usage, err := cpuUsage(); err == nil {
		metricsRegistry.Gauges.Set("node_cpu_usage", int64(usage))
	}
	n.metricsMu.Lock()
	metricsRegistry.Counters.UpdateDelta()
	n.metricsSnapshot = n.getSnapshotMetrics()
	metricsRegistry.HDRHistograms.Rotate()
	n.metricsMu.Unlock()
}

func (n *Node) updateMetrics() {
	for {
		n.mu.RLock()
		interval := n.config.NodeMetricsInterval
		n.mu.RUnlock()
		select {
		case <-n.shutdownCh:
			return
		case <-time.After(interval):
			n.updateMetricsOnce()
		}
	}
}

func (n *Node) sendNodePingMsg() {
	for {
		n.mu.RLock()
		interval := n.config.NodePingInterval
		n.mu.RUnlock()
		select {
		case <-n.shutdownCh:
			return
		case <-time.After(interval):
			err := n.pubPing()
			if err != nil {
				logger.CRITICAL.Println(err)
			}
		}
	}
}

func (n *Node) cleanNodeInfo() {
	for {
		n.mu.RLock()
		interval := n.config.NodeInfoCleanInterval
		n.mu.RUnlock()
		select {
		case <-n.shutdownCh:
			return
		case <-time.After(interval):
			n.mu.RLock()
			delay := n.config.NodeInfoMaxDelay
			n.mu.RUnlock()
			n.nodes.clean(delay)
		}
	}
}

// Channels returns list of all engines clients subscribed on all Centrifugo nodes.
func (n *Node) Channels() ([]string, error) {
	return n.engine.Channels()
}

// Stats returns aggregated stats from all Centrifugo nodes.
func (n *Node) Stats() proto.ServerStats {
	n.mu.RLock()
	interval := n.config.NodeMetricsInterval
	n.mu.RUnlock()

	return proto.ServerStats{
		MetricsInterval: int64(interval.Seconds()),
		Nodes:           n.nodes.list(),
	}
}

// Node returns raw information only from current node.
func (n *Node) Node() proto.NodeInfo {
	info := n.nodes.get(n.uid)
	info.Metrics = n.getRawMetrics()
	return info
}

func (n *Node) getRawMetrics() map[string]int64 {
	m := make(map[string]int64)
	for name, val := range metricsRegistry.Counters.LoadValues() {
		m[name] = val
	}
	for name, val := range metricsRegistry.HDRHistograms.LoadValues() {
		m[name] = val
	}
	for name, val := range metricsRegistry.Gauges.LoadValues() {
		m[name] = val
	}
	return m
}

func (n *Node) getSnapshotMetrics() map[string]int64 {
	m := make(map[string]int64)
	for name, val := range metricsRegistry.Counters.LoadIntervalValues() {
		m[name] = val
	}
	for name, val := range metricsRegistry.HDRHistograms.LoadValues() {
		m[name] = val
	}
	for name, val := range metricsRegistry.Gauges.LoadValues() {
		m[name] = val
	}
	return m
}

// ControlMsg handles messages from control channel - control messages used for internal
// communication between nodes to share state or proto.
func (n *Node) ControlMsg(cmd *proto.ControlMessage) error {
	metricsRegistry.Counters.Inc("node_num_control_msg_received")

	if cmd.UID == n.uid {
		// Sent by this node.
		return nil
	}

	method := cmd.Method
	params := cmd.Params

	switch method {
	case "ping":
		var cmd proto.PingControlCommand
		err := json.Unmarshal(params, &cmd)
		if err != nil {
			logger.ERROR.Println(err)
			return proto.ErrInvalidMessage
		}
		return n.pingCmd(&cmd)
	case "unsubscribe":
		var cmd proto.UnsubscribeControlCommand
		err := json.Unmarshal(params, &cmd)
		if err != nil {
			logger.ERROR.Println(err)
			return proto.ErrInvalidMessage
		}
		return n.unsubscribeUser(cmd.User, cmd.Channel)
	case "disconnect":
		var cmd proto.DisconnectControlCommand
		err := json.Unmarshal(params, &cmd)
		if err != nil {
			logger.ERROR.Println(err)
			return proto.ErrInvalidMessage
		}
		return n.disconnectUser(cmd.User, false)
	default:
		logger.ERROR.Println("unknown control message method", method)
		return proto.ErrInvalidMessage
	}
}

// AdminMsg handlesadmin message broadcasting it to all admins connected to this node.
func (n *Node) AdminMsg(msg *proto.AdminMessage) error {
	metricsRegistry.Counters.Inc("node_num_admin_msg_received")
	hasAdmins := n.admins.NumAdmins() > 0
	if !hasAdmins {
		return nil
	}
	resp := proto.NewAdminMessageResponse(msg.Params)
	byteMessage, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	return n.admins.Broadcast(byteMessage)
}

// ClientMsg handles messages published by web application or client into channel.
// The goal of this method to deliver this message to all clients on this node subscribed
// on channel.
func (n *Node) ClientMsg(msg *proto.Message, appkey, users, nusers string) error {
	ch := msg.Channel
	metricsRegistry.Counters.Inc("node_num_client_msg_received")
	numSubscribers := n.clients.NumSubscribers(ch)
	hasCurrentSubscribers := numSubscribers > 0
	if !hasCurrentSubscribers {
		return nil
	}
	resp := proto.NewClientMessage(msg)
	byteMessage, err := resp.Marshal()
	if err != nil {
		return err
	}
	nusers += "," + msg.Client
	return n.clients.Broadcast(ch, appkey, byteMessage, users, nusers)
}

// JoinMsg handles JoinMessage.
func (n *Node) JoinMsg(msg *proto.JoinMessage) error {
	ch := msg.Channel
	metricsRegistry.Counters.Inc("node_num_join_msg_received")
	hasCurrentSubscribers := n.clients.NumSubscribers(ch) > 0
	if !hasCurrentSubscribers {
		return nil
	}
	resp := proto.NewClientJoinMessage(msg)
	byteMessage, err := resp.Marshal()
	if err != nil {
		return err
	}
	return n.clients.Broadcast(ch, "", byteMessage, "", "")
}

// LeaveMsg handles leave message.
func (n *Node) LeaveMsg(msg *proto.LeaveMessage) error {
	ch := msg.Channel
	metricsRegistry.Counters.Inc("node_num_leave_msg_received")
	hasCurrentSubscribers := n.clients.NumSubscribers(ch) > 0
	if !hasCurrentSubscribers {
		return nil
	}
	resp := proto.NewClientLeaveMessage(msg)
	byteMessage, err := resp.Marshal()
	if err != nil {
		return err
	}
	return n.clients.Broadcast(ch, "", byteMessage, "", "")
}

func makeErrChan(err error) <-chan error {
	ret := make(chan error, 1)
	ret <- err
	return ret
}

func (n *Node) Forbidden(r raw.Raw) bool {
	return n.engine.Forbidden(r)
}

// Publish sends a message to all clients subscribed on channel. All running nodes
// will receive it and will send it to all clients on node subscribed on channel.
func (n *Node) Publish(msg *proto.Message, appkey, users, nusers string, opts *channel.Options) <-chan error {
	if opts == nil {
		chOpts, err := n.ChannelOpts(msg.Channel)
		if err != nil {
			return makeErrChan(err)
		}
		opts = &chOpts
	}
	metricsRegistry.Counters.Inc("node_num_client_msg_published")
	return n.engine.PublishMessage(msg, appkey, users, nusers, opts)
}

// PublishJoin allows to publish join message into channel when someone subscribes on it
// or leave message when someone unsubscribes from channel.
func (n *Node) PublishJoin(msg *proto.JoinMessage, opts *channel.Options) <-chan error {
	if opts == nil {
		chOpts, err := n.ChannelOpts(msg.Channel)
		if err != nil {
			return makeErrChan(err)
		}
		opts = &chOpts
	}
	metricsRegistry.Counters.Inc("node_num_join_msg_published")
	return n.engine.PublishJoin(msg, opts)
}

// PublishLeave allows to publish join message into channel when someone subscribes on it
// or leave message when someone unsubscribes from channel.
func (n *Node) PublishLeave(msg *proto.LeaveMessage, opts *channel.Options) <-chan error {
	if opts == nil {
		chOpts, err := n.ChannelOpts(msg.Channel)
		if err != nil {
			return makeErrChan(err)
		}
		opts = &chOpts
	}
	metricsRegistry.Counters.Inc("node_num_leave_msg_published")
	return n.engine.PublishLeave(msg, opts)
}

// PublishAdmin publishes message to admins.
func (n *Node) PublishAdmin(msg *proto.AdminMessage) <-chan error {
	metricsRegistry.Counters.Inc("node_num_admin_msg_published")
	return n.engine.PublishAdmin(msg)
}

// publishControl publishes message into control channel so all running
// nodes will receive and handle it.
func (n *Node) publishControl(msg *proto.ControlMessage) <-chan error {
	metricsRegistry.Counters.Inc("node_num_control_msg_published")
	return n.engine.PublishControl(msg)
}

// pubPing sends control ping message to all nodes - this message
// contains information about current node.
func (n *Node) pubPing() error {
	n.mu.RLock()
	metricsRegistry.Gauges.Set("node_num_clients", int64(n.clients.NumClients()))
	metricsRegistry.Gauges.Set("node_num_unique_clients", int64(n.clients.NumUniqueClients()))
	metricsRegistry.Gauges.Set("node_num_channels", int64(n.clients.NumChannels()))
	metricsRegistry.Gauges.Set("node_num_goroutine", int64(runtime.NumGoroutine()))
	metricsRegistry.Gauges.Set("node_uptime_seconds", time.Now().Unix()-n.started)

	metricsSnapshot := make(map[string]int64)
	n.metricsMu.RLock()
	for k, v := range n.metricsSnapshot {
		metricsSnapshot[k] = v
	}
	n.metricsMu.RUnlock()

	info := proto.NodeInfo{
		UID:     n.uid,
		Name:    n.config.Name,
		Started: n.started,
		Metrics: metricsSnapshot,
	}
	n.mu.RUnlock()

	cmd := &proto.PingControlCommand{Info: info}

	err := n.pingCmd(cmd)
	if err != nil {
		logger.ERROR.Println(err)
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	return <-n.publishControl(proto.NewControlMessage(n.uid, "ping", cmdBytes))
}

// pubUnsubscribe publishes unsubscribe control message to all nodes – so all
// nodes could unsubscribe user from channel.
func (n *Node) pubUnsubscribe(user string, ch string) error {

	cmd := &proto.UnsubscribeControlCommand{
		User:    user,
		Channel: ch,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	return <-n.publishControl(proto.NewControlMessage(n.uid, "unsubscribe", cmdBytes))
}

// pubDisconnect publishes disconnect control message to all nodes – so all
// nodes could disconnect user from Centrifugo.
func (n *Node) pubDisconnect(user string, reconnect bool) error {

	cmd := &proto.DisconnectControlCommand{
		User: user,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	return <-n.publishControl(proto.NewControlMessage(n.uid, "disconnect", cmdBytes))
}

// AddClientConn registers authenticated connection in clientConnectionHub
// this allows to make operations with user connection on demand.
func (n *Node) AddClientConn(c conns.ClientConn) error {
	metricsRegistry.Counters.Inc("node_num_add_client_conn")
	return n.clients.Add(c)
}

// RemoveClientConn removes client connection from connection registry.
func (n *Node) RemoveClientConn(c conns.ClientConn) error {
	metricsRegistry.Counters.Inc("node_num_remove_client_conn")
	return n.clients.Remove(c)
}

// AddClientSub registers subscription of connection on channel in both
// engine and clientSubscriptionHub.
func (n *Node) AddClientSub(ch string, c conns.ClientConn) error {
	metricsRegistry.Counters.Inc("node_num_add_client_sub")
	first, err := n.clients.AddSub(ch, c)
	if err != nil {
		return err
	}
	if first {
		return n.engine.Subscribe(ch)
	}
	return nil
}

// RemoveClientSub removes subscription of connection on channel
// from both engine and clientSubscriptionHub.
func (n *Node) RemoveClientSub(ch string, c conns.ClientConn) error {
	metricsRegistry.Counters.Inc("node_num_remove_client_sub")
	empty, err := n.clients.RemoveSub(ch, c)
	if err != nil {
		return err
	}
	if empty {
		return n.engine.Unsubscribe(ch)
	}
	return nil
}

// pingCmd handles ping control command i.e. updates information about known nodes.
func (n *Node) pingCmd(cmd *proto.PingControlCommand) error {
	info := cmd.Info
	n.nodes.add(info)
	return nil
}

// Unsubscribe unsubscribes user from channel, if channel is equal to empty
// string then user will be unsubscribed from all channels.
func (n *Node) Unsubscribe(user string, ch string) error {

	if string(user) == "" {
		return proto.ErrInvalidMessage
	}

	if string(ch) != "" {
		_, err := n.ChannelOpts(ch)
		if err != nil {
			return err
		}
	}

	// First unsubscribe on this node.
	err := n.unsubscribeUser(user, ch)
	if err != nil {
		return proto.ErrInternalServerError
	}
	// Second send unsubscribe control message to other nodes.
	err = n.pubUnsubscribe(user, ch)
	if err != nil {
		return proto.ErrInternalServerError
	}
	return nil
}

// unsubscribeUser unsubscribes user from channel on this node. If channel
// is an empty string then user will be unsubscribed from all channels.
func (n *Node) unsubscribeUser(user string, ch string) error {
	userConnections := n.clients.UserConnections(user)
	for _, c := range userConnections {
		var channels []string
		if string(ch) == "" {
			// unsubscribe from all channels
			channels = c.Channels()
		} else {
			channels = []string{ch}
		}

		for _, channel := range channels {
			err := c.Unsubscribe(channel)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Disconnect allows to close all user connections to Centrifugo.
func (n *Node) Disconnect(user string, reconnect bool) error {

	if string(user) == "" {
		return proto.ErrInvalidMessage
	}

	// first disconnect user from this node
	err := n.disconnectUser(user, reconnect)
	if err != nil {
		return proto.ErrInternalServerError
	}
	// second send disconnect control message to other nodes
	err = n.pubDisconnect(user, reconnect)
	if err != nil {
		return proto.ErrInternalServerError
	}
	return nil
}

// disconnectUser closes client connections of user on current node.
func (n *Node) disconnectUser(user string, reconnect bool) error {
	userConnections := n.clients.UserConnections(user)
	advice := &conns.DisconnectAdvice{Reason: "disconnect", Reconnect: reconnect}
	for _, c := range userConnections {
		go func(cc conns.ClientConn) {
			cc.Close(advice)
		}(c)
	}
	return nil
}

// namespaceKey returns namespace key from channel name if exists.
func (n *Node) namespaceKey(ch string) channel.NamespaceKey {
	cTrim := strings.TrimPrefix(ch, n.config.PrivateChannelPrefix)
	if strings.Contains(cTrim, n.config.NamespaceChannelBoundary) {
		parts := strings.SplitN(cTrim, n.config.NamespaceChannelBoundary, 2)
		return channel.NamespaceKey(parts[0])
	}
	return channel.NamespaceKey("")
}

// ChannelOpts returns channel options for channel using current channel config.
func (n *Node) ChannelOpts(ch string) (channel.Options, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.config.channelOpts(n.namespaceKey(ch))
}

// AddPresence proxies presence adding to engine.
func (n *Node) AddPresence(ch string, uid string, info proto.ClientInfo) error {
	n.mu.RLock()
	expire := int(n.config.PresenceExpireInterval.Seconds())
	n.mu.RUnlock()
	metricsRegistry.Counters.Inc("node_num_add_presence")
	return n.engine.AddPresence(ch, uid, info, expire)
}

// RemovePresence proxies presence removing to engine.
func (n *Node) RemovePresence(ch, uid, user string) error {
	metricsRegistry.Counters.Inc("node_num_remove_presence")
	return n.engine.RemovePresence(ch, uid, user)
}

// Presence returns a map of active clients in project channel.
func (n *Node) Presence(ch string) (map[string]proto.ClientInfo, error) {

	if string(ch) == "" {
		return map[string]proto.ClientInfo{}, proto.ErrInvalidMessage
	}

	chOpts, err := n.ChannelOpts(ch)
	if err != nil {
		return map[string]proto.ClientInfo{}, err
	}

	if !chOpts.Presence {
		return map[string]proto.ClientInfo{}, proto.ErrNotAvailable
	}

	metricsRegistry.Counters.Inc("node_num_presence")

	presence, err := n.engine.Presence(ch)
	if err != nil {
		logger.ERROR.Println(err)
		return map[string]proto.ClientInfo{}, proto.ErrInternalServerError
	}
	return presence, nil
}
func (n *Node) ReadMessage(ch, msgid, uid string) (bool, error) {
	return n.engine.ReadMessage(ch, msgid, uid)
}

// History returns a slice of last messages published into project channel.
func (n *Node) History(ch, appkey, client string, skip, limit int) ([]proto.Message, int, error) {

	if string(ch) == "" {
		return []proto.Message{}, 0, proto.ErrInvalidMessage
	}

	// chOpts, err := n.ChannelOpts(ch)
	// if err != nil {
	// 	return []proto.Message{}, 0, err
	// }

	// if chOpts.HistorySize <= 0 || chOpts.HistoryLifetime <= 0 {
	// 	return []proto.Message{}, 0, proto.ErrNotAvailable
	// }

	metricsRegistry.Counters.Inc("node_num_history")

	history, total, err := n.engine.History(ch, appkey, client, skip, limit)
	if err != nil {
		logger.ERROR.Println(err)
		return []proto.Message{}, 0, proto.ErrInternalServerError
	}
	return history, total, nil
}

// LastMessageID return last message id for channel.
func (n *Node) LastMessageID(ch, appkey, client string) (string, error) {
	metricsRegistry.Counters.Inc("node_num_last_message_id")
	history, _, err := n.engine.History(ch, appkey, client, -1, 1)
	if err != nil {
		return "", err
	}
	if len(history) == 0 {
		return "", nil
	}
	return history[0].UID, nil
}
func (n *Node) Micro(connid string, cmd proto.MicroCommand) {
	metricsRegistry.Counters.Inc("node_num_micro")
	n.engine.Micro(connid, cmd)
}

// PrivateChannel checks if channel private and therefore subscription
// request on it must be properly signed on web application backend.
func (n *Node) PrivateChannel(ch string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return strings.HasPrefix(string(ch), n.config.PrivateChannelPrefix)
}

// UserAllowed checks if user can subscribe on channel - as channel
// can contain special part in the end to indicate which users allowed
// to subscribe on it.
func (n *Node) UserAllowed(ch string, user string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !strings.Contains(ch, n.config.UserChannelBoundary) {
		return true
	}
	parts := strings.Split(ch, n.config.UserChannelBoundary)
	allowedUsers := strings.Split(parts[len(parts)-1], n.config.UserChannelSeparator)
	for _, allowedUser := range allowedUsers {
		if string(user) == allowedUser {
			return true
		}
	}
	return false
}

// ClientAllowed checks if client can subscribe on channel - as channel
// can contain special part in the end to indicate which client allowed
// to subscribe on it.
func (n *Node) ClientAllowed(ch string, client string) bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if !strings.Contains(ch, n.config.ClientChannelBoundary) {
		return true
	}
	parts := strings.Split(ch, n.config.ClientChannelBoundary)
	allowedClient := parts[len(parts)-1]
	if string(client) == allowedClient {
		return true
	}
	return false
}

type nodeRegistry struct {
	// mu allows to synchronize access to node registry.
	mu sync.RWMutex
	// currentUID keeps uid of current node
	currentUID string
	// nodes is a map with information about known nodes.
	nodes map[string]proto.NodeInfo
	// updates track time we last received ping from node. Used to clean up nodes map.
	updates map[string]int64
}

func newNodeRegistry(currentUID string) *nodeRegistry {
	return &nodeRegistry{
		currentUID: currentUID,
		nodes:      make(map[string]proto.NodeInfo),
		updates:    make(map[string]int64),
	}
}

func (r *nodeRegistry) list() []proto.NodeInfo {
	r.mu.RLock()
	nodes := make([]proto.NodeInfo, len(r.nodes))
	i := 0
	for _, info := range r.nodes {
		nodes[i] = info
		i++
	}
	r.mu.RUnlock()
	return nodes
}

func (r *nodeRegistry) get(uid string) proto.NodeInfo {
	r.mu.RLock()
	info, _ := r.nodes[uid]
	r.mu.RUnlock()
	return info
}

func (r *nodeRegistry) add(info proto.NodeInfo) {
	r.mu.Lock()
	r.nodes[info.UID] = info
	r.updates[info.UID] = time.Now().Unix()
	r.mu.Unlock()
}

func (r *nodeRegistry) clean(delay time.Duration) {
	r.mu.Lock()
	for uid := range r.nodes {
		if uid == r.currentUID {
			// No need to clean info for current node.
			continue
		}
		updated, ok := r.updates[uid]
		if !ok {
			// As we do all operations with nodes under lock this should never happen.
			delete(r.nodes, uid)
			continue
		}
		if time.Now().Unix()-updated > int64(delay.Seconds()) {
			// Too many seconds since this node have been last seen - remove it from map.
			delete(r.nodes, uid)
			delete(r.updates, uid)
		}
	}
	r.mu.Unlock()
}
