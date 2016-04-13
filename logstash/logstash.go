package logstash

import (
	"fmt"
	"net"
	"sync"
	"time"
)

var defaultTimeout = time.Second * 2
var defaultRetries = 5

// Client is a logstash client
type Client struct {
	Timeout time.Duration
	Retries int
	addr    *net.TCPAddr
	conn    net.Conn
	mu      *sync.RWMutex
}

// Connect attempts to establish a TCP connection to the specified host port
func Connect(host string, port int) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve logstash address %s: %s", addr, err)
	}

	c := &Client{
		Timeout: defaultTimeout,
		Retries: defaultRetries,
		addr:    tcpAddr,
		mu:      &sync.RWMutex{},
	}

	if err := c.reconnect(); err != nil {
		return nil, err
	}
	return c, nil
}

// reconnect replaces the Client's net.Conn or returns an error if it can't connect
func (c *Client) reconnect() error {
	tcpClient, err := net.DialTimeout("tcp4", c.addr.String(), time.Second*4)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %s", c.addr.String(), err)
	}
	c.mu.Lock()
	c.conn = tcpClient
	c.mu.Unlock()
	return nil
}

// Send will make Retries attempts to send a message re-connecting in between attempts
// There is no parsing so the message should be formatted how your logstash instance expects it
func (c *Client) Send(message []byte) error {
	var err error
	for i := 0; i < c.Retries; i++ {
		c.mu.RLock()
		if c.conn == nil {
			return fmt.Errorf("connection not initialized")
		}
		c.conn.SetDeadline(time.Now().Add(c.Timeout))
		_, err = c.conn.Write(message)
		c.mu.RUnlock()
		if err != nil {
			c.reconnect()
			time.Sleep(time.Second * 1)
			continue
		}
		return nil
	}
	return fmt.Errorf("all %d retries failed: %s", c.Retries, err)
}
