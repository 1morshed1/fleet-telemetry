// Package dashboard serves the REST + WebSocket API and fans out live vehicle
// positions from Redis Pub/Sub to every connected browser.
package dashboard

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"github.com/1morshed1/fleet-telemetry/internal/live"
)

// Hub bridges one Redis subscription to many WebSocket clients. To protect the
// browser (and the network) it COALESCES updates: it keeps only the latest
// position per vehicle and broadcasts a batch on a fixed interval, rather than
// forwarding every single ping. This caps client traffic at
// (vehicles updated) per (broadcast interval) regardless of ingest rate.
type Hub struct {
	rdb      *redis.Client
	interval time.Duration

	mu      sync.Mutex
	clients map[*client]struct{}
	latest  map[string]live.Position
	dirty   map[string]struct{}
}

type client struct {
	conn *websocket.Conn
	send chan []byte
}

// NewHub creates a hub that broadcasts every interval.
func NewHub(rdb *redis.Client, interval time.Duration) *Hub {
	return &Hub{
		rdb:      rdb,
		interval: interval,
		clients:  make(map[*client]struct{}),
		latest:   make(map[string]live.Position),
		dirty:    make(map[string]struct{}),
	}
}

// Run subscribes to Redis and drives the broadcast ticker until ctx is done.
func (h *Hub) Run(ctx context.Context) {
	sub := h.rdb.Subscribe(ctx, live.Channel)
	defer sub.Close()
	ch := sub.Channel()

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			var pos live.Position
			if err := json.Unmarshal([]byte(msg.Payload), &pos); err != nil {
				continue
			}
			h.mu.Lock()
			h.latest[pos.VehicleID] = pos
			h.dirty[pos.VehicleID] = struct{}{}
			h.mu.Unlock()
		case <-ticker.C:
			h.broadcast()
		}
	}
}

// broadcast sends all positions that changed since the last tick to every client.
func (h *Hub) broadcast() {
	h.mu.Lock()
	if len(h.dirty) == 0 || len(h.clients) == 0 {
		// Still clear dirty so we don't accumulate unboundedly with no clients.
		h.dirty = make(map[string]struct{})
		h.mu.Unlock()
		return
	}
	batch := make([]live.Position, 0, len(h.dirty))
	for id := range h.dirty {
		batch = append(batch, h.latest[id])
	}
	h.dirty = make(map[string]struct{})
	clients := make([]*client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()

	payload, err := json.Marshal(batch)
	if err != nil {
		return
	}
	for _, c := range clients {
		select {
		case c.send <- payload:
		default:
			// Slow client: drop this update rather than block the whole hub.
			// (A persistently slow client will be closed by its write timeout.)
		}
	}
}

func (h *Hub) addClient(c *client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) removeClient(c *client) {
	h.mu.Lock()
	if _, ok := h.clients[c]; ok {
		delete(h.clients, c)
		close(c.send)
	}
	h.mu.Unlock()
}

// snapshot returns the current latest position of every known vehicle, so a
// newly-connected client gets the full picture immediately instead of waiting
// for each vehicle to move.
func (h *Hub) snapshot() []live.Position {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]live.Position, 0, len(h.latest))
	for _, p := range h.latest {
		out = append(out, p)
	}
	return out
}

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 30 * time.Second
)

// serveWS upgrades an HTTP request to a WebSocket and runs the read/write pumps.
func (h *Hub) serveWS(conn *websocket.Conn) {
	c := &client{conn: conn, send: make(chan []byte, 32)}
	h.addClient(c)

	// Send the initial snapshot.
	if snap := h.snapshot(); len(snap) > 0 {
		if b, err := json.Marshal(snap); err == nil {
			select {
			case c.send <- b:
			default:
			}
		}
	}

	go h.writePump(c)
	h.readPump(c) // blocks until the client disconnects
}

// readPump discards inbound frames (clients don't send us data) and detects
// disconnects / keeps the connection alive via pong deadlines.
func (h *Hub) readPump(c *client) {
	defer func() {
		h.removeClient(c)
		c.conn.Close()
	}()
	c.conn.SetReadLimit(512)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}

// writePump sends broadcast batches and periodic pings to one client.
func (h *Hub) writePump(c *client) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case msg, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok { // hub closed the channel
				_ = c.conn.WriteMessage(websocket.CloseMessage, nil)
				return
			}
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
