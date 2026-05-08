package cluster

import (
	"fmt"
	"net"
	"strconv"

	"github.com/hashicorp/memberlist"
)

type Gossip struct {
	list *memberlist.Memberlist
}

func NewGossip(bindAddr, nodeName string, joinAddrs []string) (*Gossip, error) {
	config := memberlist.DefaultLocalConfig()
	config.Name = nodeName

	host, portStr, err := net.SplitHostPort(bindAddr)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	config.BindAddr = host
	config.BindPort = port

	list, err := memberlist.Create(config)
	if err != nil {
		return nil, err
	}

	if len(joinAddrs) > 0 {
		_, err = list.Join(joinAddrs)
		if err != nil {
			list.Shutdown()
			return nil, err
		}
	}

	return &Gossip{list: list}, nil
}

func (g *Gossip) BindAddr() string {
	return fmt.Sprintf("%s:%d", g.list.LocalNode().Addr, g.list.LocalNode().Port)
}

func (g *Gossip) Members() []*memberlist.Node {
	return g.list.Members()
}

func (g *Gossip) Shutdown() error {
	return g.list.Shutdown()
}
