package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Message payload structure sent across the PubSub network
type ChatMessage struct {
	SenderID   string `json:"sender_id"`
	SenderNick string `json:"sender_nick"`
	Message    string `json:"message"`
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("[System] Initializing P2P Node with GossipSub...")

	// 1. Initialize P2P Host
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.EnableNATService(),
	)
	if err != nil {
		panic(err)
	}
	defer h.Close()

	// 2. Connect to global DHT network for peer discovery
	fmt.Println("[System] Connecting to DHT bootstrap nodes...")
	kadDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
	if err != nil {
		panic(err)
	}

	var wg sync.WaitGroup
	for _, addrInfo := range dht.GetDefaultBootstrapPeerAddrInfos() {
		wg.Add(1)
		go func(info peer.AddrInfo) {
			defer wg.Done()
			if err := h.Connect(ctx, info); err == nil {
				kadDHT.Bootstrap(ctx)
			}
		}(addrInfo)
	}
	wg.Wait()

	// 3. Initialize the GossipSub Router
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		panic(err)
	}

	fmt.Println("\n=================== YOUR NODE DETAILS ===================")
	fmt.Printf("Your Peer ID: %s\n", h.ID().String())
	fmt.Println("=========================================================")

	reader := bufio.NewReader(os.Stdin)

	// Set local display nickname
	fmt.Print("\nEnter your nickname: ")
	nickInput, _ := reader.ReadString('\n')
	nickname := strings.TrimSpace(nickInput)
	if nickname == "" {
		nickname = h.ID().ShortString()
	}

	// Choose Room
	fmt.Print("Enter room name to join (default: global): ")
	roomInput, _ := reader.ReadString('\n')
	roomName := strings.TrimSpace(roomInput)
	if roomName == "" {
		roomName = "global"
	}

	// 4. Optional: Allow connecting to a seed peer directly by Peer ID
	fmt.Print("Enter a friend's Peer ID to discover the room faster (or leave blank): ")
	seedInput, _ := reader.ReadString('\n')
	seedPeerIDStr := strings.TrimSpace(seedInput)

	if seedPeerIDStr != "" {
		targetPeerID, err := peer.Decode(seedPeerIDStr)
		if err == nil {
			fmt.Println("[System] Resolving seed peer location via DHT...")
			lookupCtx, lookupCancel := context.WithTimeout(ctx, 15*time.Second)
			peerInfo, err := kadDHT.FindPeer(lookupCtx, targetPeerID)
			lookupCancel()

			if err == nil {
				h.Connect(ctx, peerInfo)
				fmt.Println("[System] Connected to seed peer!")
			} else {
				fmt.Printf("[Warning] Could not find seed peer on DHT: %v\n", err)
			}
		}
	}

	// 5. Join the GossipSub Topic
	topic, err := ps.Join(roomName)
	if err != nil {
		panic(err)
	}
	defer topic.Close()

	sub, err := topic.Subscribe()
	if err != nil {
		panic(err)
	}
	defer sub.Cancel()

	fmt.Printf("\n[System] Joined room '%s' as '%s'. Start typing to broadcast!\n", roomName, nickname)
	fmt.Println("---------------------------------------------------------")

	// Start reading inbound messages from the pubsub subscription
	go readFromRoom(ctx, sub, h.ID().String())

	// SINGLE STDIN LOOP (Broadcasting to the topic)
	for {
		fmt.Print("You: ")
		msgText, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		msgText = strings.TrimSpace(msgText)
		if msgText == "" {
			continue
		}

		cm := ChatMessage{
			SenderID:   h.ID().String(),
			SenderNick: nickname,
			Message:    msgText,
		}

		msgBytes, err := json.Marshal(cm)
		if err != nil {
			fmt.Printf("[Error] Failed to format message: %v\n", err)
			continue
		}

		// Publish to the entire room mesh
		err = topic.Publish(ctx, msgBytes)
		if err != nil {
			fmt.Printf("[Error] Publish failed: %v\n", err)
		}
	}
}

// Background goroutine to listen to the GossipSub stream
func readFromRoom(ctx context.Context, sub *pubsub.Subscription, myID string) {
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}

		// Don't print messages originating from ourselves
		if msg.ReceivedFrom.String() == myID {
			continue
		}

		var cm ChatMessage
		if err := json.Unmarshal(msg.Data, &cm); err != nil {
			continue
		}

		// Clear local input prompt, print incoming group message, restore prompt
		fmt.Printf("\r[%s]: %s\nYou: ", cm.SenderNick, cm.Message)
	}
}
