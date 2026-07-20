package main

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"

	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const protocolID = "/p2p-chat-internet/1.3.0"

// State management
type ChatEngine struct {
	host          host.Host
	kadDHT        *dht.IpfsDHT
	activeStream  network.Stream
	streamMutex   sync.Mutex
	isChatActive  bool
	
	// Channels bridging P2P backend to Fyne UI
	incomingMsgs  chan string
	systemLogs    chan string
}

func main() {
	// Initialize UI App
	myApp := app.New()
	myWindow := myApp.NewWindow("P2P Encrypted Messenger (1-on-1)")
	myWindow.Resize(fyne.NewSize(600, 500))

	engine := &ChatEngine{
		incomingMsgs: make(chan string, 100),
		systemLogs:   make(chan string, 100),
	}

	// --- UI WIDGETS ---
	chatHistory := widget.NewMultiLineEntry()
	chatHistory.Disable() // Read-only

	statusLabel := widget.NewLabel("Status: Initializing P2P Node...")
	myPeerIDEntry := widget.NewEntry()
	myPeerIDEntry.Disable() // Displays our own ID

	targetPeerEntry := widget.NewEntry()
	targetPeerEntry.SetPlaceHolder("Paste friend's Peer ID here...")

	msgEntry := widget.NewEntry()
	msgEntry.SetPlaceHolder("Type your message...")

	connectBtn := widget.NewButton("Connect", nil)
	sendBtn := widget.NewButton("Send", nil)

	// --- LAYOUT SETUP ---
	topBox := container.NewVBox(
		widget.NewLabel("Your Peer ID (Share this):"),
		myPeerIDEntry,
		widget.NewLabel("Connect to Peer:"),
		container.NewBorder(nil, nil, nil, connectBtn, targetPeerEntry),
		statusLabel,
	)

	bottomBox := container.NewBorder(nil, nil, nil, sendBtn, msgEntry)
	mainLayout := container.NewBorder(topBox, bottomBox, nil, nil, container.NewScroll(chatHistory))
	myWindow.SetContent(mainLayout)

	// --- ASYNC NETWORK INITIALIZATION ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		engine.initP2P(ctx)
		
		// Update UI with our generated Peer ID
		myPeerIDEntry.SetText(engine.host.ID().String())
		engine.systemLogs <- "Connected to global DHT network. Ready!"
	}()

	// --- UI EVENT HANDLERS ---

	// Click "Connect" button
	connectBtn.OnTapped = func() {
		targetIDStr := strings.TrimSpace(targetPeerEntry.Text)
		if targetIDStr == "" {
			return
		}

		connectBtn.Disable()
		statusLabel.SetText("Status: Resolving peer on DHT...")

		go func() {
			err := engine.connectToPeer(ctx, targetIDStr)
			if err != nil {
				engine.systemLogs <- fmt.Sprintf("Error: %v", err)
			} else {
				engine.systemLogs <- "Connected! You can now send messages."
			}
			connectBtn.Enable()
		}()
	}

	// Click "Send" button
	sendMessage := func() {
		text := strings.TrimSpace(msgEntry.Text)
		if text == "" {
			return
		}

		engine.streamMutex.Lock()
		s := engine.activeStream
		active := engine.isChatActive
		engine.streamMutex.Unlock()

		if !active || s == nil {
			engine.systemLogs <- "Cannot send: No active peer connection."
			return
		}

		_, err := s.Write([]byte(text + "\n"))
		if err != nil {
			engine.systemLogs <- "Failed to send message: Connection lost."
			return
		}

		chatHistory.SetText(chatHistory.Text + "\nYou: " + text)
		msgEntry.SetText("")
	}

	sendBtn.OnTapped = sendMessage
	msgEntry.OnSubmitted = func(_ string) { sendMessage() }

	// --- CHANNEL LISTENER (Updates UI components safely) ---
	go func() {
		for {
			select {
			case msg := <-engine.incomingMsgs:
				chatHistory.SetText(chatHistory.Text + "\nPeer: " + msg)
			case log := <-engine.systemLogs:
				statusLabel.SetText("Status: " + log)
			}
		}
	}()

	myWindow.ShowAndRun()
}

// --- P2P BACKEND METHODS ---

func (e *ChatEngine) initP2P(ctx context.Context) {
	h, err := libp2p.New(
		libp2p.ListenAddrStrings("/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
		libp2p.EnableNATService(),
	)
	if err != nil {
		panic(err)
	}
	e.host = h

	// Handle Inbound Connections
	h.SetStreamHandler(protocolID, func(s network.Stream) {
		e.streamMutex.Lock()
		if e.isChatActive {
			s.Reset()
			e.streamMutex.Unlock()
			return
		}
		e.activeStream = s
		e.isChatActive = true
		e.streamMutex.Unlock()

		e.systemLogs <- fmt.Sprintf("Inbound connection from %s", s.Conn().RemotePeer().ShortString())
		go e.readFromStream(s)
	})

	// Bootstrap DHT
	kadDHT, err := dht.New(ctx, h, dht.Mode(dht.ModeServer))
	if err != nil {
		panic(err)
	}
	e.kadDHT = kadDHT

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
}

func (e *ChatEngine) connectToPeer(ctx context.Context, peerIDStr string) error {
	targetPeerID, err := peer.Decode(peerIDStr)
	if err != nil {
		return fmt.Errorf("invalid Peer ID format")
	}

	lookupCtx, lookupCancel := context.WithTimeout(ctx, 20*time.Second)
	defer lookupCancel()

	peerInfo, err := e.kadDHT.FindPeer(lookupCtx, targetPeerID)
	if err != nil {
		return fmt.Errorf("could not find peer on DHT")
	}

	connectCtx, connectCancel := context.WithTimeout(ctx, 15*time.Second)
	defer connectCancel()

	if err := e.host.Connect(connectCtx, peerInfo); err != nil {
		return fmt.Errorf("handshake failed: %w", err)
	}

	s, err := e.host.NewStream(ctx, peerInfo.ID, protocolID)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}

	e.streamMutex.Lock()
	e.activeStream = s
	e.isChatActive = true
	e.streamMutex.Unlock()

	go e.readFromStream(s)
	return nil
}

func (e *ChatEngine) readFromStream(s network.Stream) {
	defer s.Close()
	reader := bufio.NewReader(s)
	for {
		str, err := reader.ReadString('\n')
		if err != nil {
			e.systemLogs <- "Peer disconnected."
			e.streamMutex.Lock()
			e.isChatActive = false
			e.activeStream = nil
			e.streamMutex.Unlock()
			return
		}
		if strings.TrimSpace(str) != "" {
			e.incomingMsgs <- strings.TrimSpace(str)
		}
	}
}