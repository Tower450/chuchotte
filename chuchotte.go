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
)

const protocolID = "/p2p-chat-internet/1.3.0"

var (
	activeStream network.Stream
	streamMutex  sync.Mutex
	isChatActive = false
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fmt.Println("[System] Initializing libp2p host with DHT discovery...")

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

	// 2. Handle Inbound Streams
	h.SetStreamHandler(protocolID, func(s network.Stream) {
		streamMutex.Lock()
		if isChatActive {
			s.Reset()
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

	// 3. Initialize Kademlia DHT in Server Mode (so we can advertise our addresses)
	fmt.Println("[System] Connecting to global DHT network...")
	kadDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
	if err != nil {
		panic(err)
	}

	// Bootstrap onto public nodes
	var wg sync.WaitGroup
	for _, addrInfo := range dht.GetDefaultBootstrapPeerAddrInfos() {
		wg.Add(1)
		go func(info peerstore.AddrInfo) {
			defer wg.Done()
			if err := h.Connect(ctx, info); err == nil {
				kadDHT.Bootstrap(ctx)
			}
		}(addrInfo)
	}
	wg.Wait()

	fmt.Println("[System] Connected to DHT network!")
	fmt.Println("\n=================== YOUR PEER ID ===================")
	fmt.Printf("Share ONLY this ID with your friend:\n%s\n", h.ID().String())
	fmt.Println("====================================================")

	// SINGLE STDIN LOOP
	stdinReader := bufio.NewReader(os.Stdin)
	fmt.Println("\nEnter friend's Peer ID to connect (or press ENTER to wait):")
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
			// CHAT MODE
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
			// PEER ID DISCOVERY MODE
			targetPeerIDStr := strings.TrimSpace(input)
			if targetPeerIDStr == "" {
				fmt.Println("[System] Waiting silently for incoming connections...")
				continue
			}

			// Parse Peer ID
			targetPeerID, err := peerstore.Decode(targetPeerIDStr)
			if err != nil {
				fmt.Printf("[Error] Invalid Peer ID format: %v. Try again:\n> ", err)
				continue
			}

			fmt.Println("[System] Searching global DHT for peer addresses...")
			lookupCtx, lookupCancel := context.WithTimeout(ctx, 30*time.Second)

			// Magic Happens Here: Ask the DHT to resolve the Peer ID into multiaddresses
			peerInfo, err := kadDHT.FindPeer(lookupCtx, targetPeerID)
			lookupCancel()

			if err != nil {
				fmt.Printf("[Error] Could not find peer on DHT: %v\nTry again or wait:\n> ", err)
				continue
			}

			fmt.Printf("[System] Peer found! Addresses resolved: %d\n", len(peerInfo.Addrs))
			fmt.Println("[System] Routing connection path...")

			connectCtx, connectCancel := context.WithTimeout(ctx, 20*time.Second)
			if err := h.Connect(connectCtx, peerInfo); err != nil {
				fmt.Printf("[Error] Connection failed: %v\nTry again or wait:\n> ", err)
				connectCancel()
				continue
			}
			connectCancel()

			s, err := h.NewStream(ctx, peerInfo.ID, protocolID)
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
			fmt.Printf("\rPeer: %sYou: ", str)
		}
	}
}