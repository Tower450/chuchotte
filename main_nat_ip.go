package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/network"
	peerstore "github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

const protocolID = "/p2p-chat-internet/1.2.0"

// Global state to coordinate our single Stdin reader
var (
	activeStream network.Stream
	streamMutex  sync.Mutex
	isChatActive = false
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("[System] Initializing libp2p host with NAT traversal...")

	// Initialize the P2P Host with Relay and Hole Punching enabled
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

	// Handle INBOUND connections
	h.SetStreamHandler(protocolID, func(s network.Stream) {
		streamMutex.Lock()
		if isChatActive {
			s.Reset() // Already in a chat, reject extra incoming streams
			streamMutex.Unlock()
			return
		}
		activeStream = s
		isChatActive = true
		streamMutex.Unlock()

		fmt.Printf("\n[System] Inbound connection secured from: %s\n", s.Conn().RemotePeer())
		fmt.Print("Press ENTER once to activate Chat Mode!\n")
		go readFromPeer(s)
	})

	// Connect to Public Bootstrap Nodes for NAT traversal assistance
	fmt.Println("[System] Connecting to public bootstrap nodes...")
	kadDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeClient))
	if err != nil {
		panic(err)
	}
	for _, addrStr := range dht.DefaultBootstrapPeers {
		addr, _ := multiaddr.NewMultiaddr(addrStr.String())
		peerinfo, _ := peerstore.AddrInfoFromP2pAddr(addr)
		go func(info peerstore.AddrInfo) {
			if err := h.Connect(ctx, info); err == nil {
				kadDHT.Bootstrap(ctx)
			}
		}(*peerinfo)
	}

	// Brief wait for DHT bootstrap routing table to populate
	time.Sleep(2 * time.Second)

	fmt.Println("\n=================== YOUR P2P ADDRESSES ===================")
	for _, addr := range h.Addrs() {
		if !strings.Contains(addr.String(), "/127.0.0.1/") {
			fmt.Printf("%s/p2p/%s\n", addr, h.ID())
		}
	}
	fmt.Println("==========================================================")

	// SINGLE STDIN LOOP: This is the ONLY place reading from your keyboard
	stdinReader := bufio.NewReader(os.Stdin)
	fmt.Println("\nEnter friend's address to connect (or leave blank and wait for them):")
	fmt.Print("> ")

	for {
		input, err := stdinReader.ReadString('\n')
		if err != nil {
			fmt.Printf("Terminal error: %v\n", err)
			return
		}

		streamMutex.Lock()
		chatting := isChatActive
		stream := activeStream
		streamMutex.Unlock()

		if chatting {
			// CHAT MODE: Send text directly to the active stream
			// If it's a blank enter from transitioning modes, just print the prompt
			if strings.TrimSpace(input) == "" {
				fmt.Print("You: ")
				continue
			}
			_, err := stream.Write([]byte(input))
			if err != nil {
				fmt.Printf("\n[System] Failed to send message: %v\n", err)
				return
			}
			fmt.Print("You: ")
		} else {
			// ADDRESS MODE: Parse target string to start a connection
			targetStr := strings.TrimSpace(input)
			if targetStr == "" {
				fmt.Println("[System] Waiting silently for incoming connections...")
				continue
			}

			maddr, err := multiaddr.NewMultiaddr(targetStr)
			if err != nil {
				fmt.Printf("[Error] %v. Try again:\n> ", err)
				continue
			}

			info, err := peerstore.AddrInfoFromP2pAddr(maddr)
			if err != nil {
				fmt.Printf("[Error] Failed to parse target info: %v. Try again:\n> ", err)
				continue
			}

			fmt.Println("[System] Routing connection path...")
			connectCtx, connectCancel := context.WithTimeout(ctx, 30*time.Second)
			if err := h.Connect(connectCtx, *info); err != nil {
				fmt.Printf("[Error] Connection failed: %v\nTry again or wait:\n> ", err)
				connectCancel()
				continue
			}
			connectCancel()

			s, err := h.NewStream(ctx, info.ID, protocolID)
			if err != nil {
				fmt.Printf("[Error] Failed to open stream: %v\n> ", err)
				continue
			}

			streamMutex.Lock()
			activeStream = s
			isChatActive = true
			streamMutex.Unlock()

			fmt.Println("[System] Outbound connection established successfully!")
			fmt.Print("You: ")
			go readFromPeer(s)
		}
	}
}

// Background reader processing incoming messages from the network stream
func readFromPeer(s network.Stream) {
	defer s.Close()
	netReader := bufio.NewReader(s)
	for {
		str, err := netReader.ReadString('\n')
		if err != nil {
			fmt.Println("\n[System] Connection lost with peer.")
			os.Exit(0)
		}
		if str != "" {
			// Clear line visual text, write the incoming data, re-print local chat prompt
			fmt.Printf("\rPeer: %sYou: ", str)
		}
	}
}