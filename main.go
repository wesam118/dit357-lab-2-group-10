package main

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

const (
	truckNamePrefix = "truck"
	fireNamePrefix  = "fire"
	managerTCPAddr  = "127.0.0.1:9000"
)

func flatTruckName(id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" {
		return truckNamePrefix
	}
	lower := strings.ToLower(trimmed)
	prefix := truckNamePrefix + "-"
	if strings.HasPrefix(lower, prefix) {
		return lower
	}
	return prefix + lower
}

func flatFireName(x, y int) string {
	return fmt.Sprintf("%s-%d-%d", fireNamePrefix, x, y)
}

func stampNow() string {
	return time.Now().Format("15:04:05.000")
}

type WaterMutexState struct {
	mu         sync.Mutex
	cond       *sync.Cond
	requesting bool
	requestTS  int
}

func newWaterMutexState() *WaterMutexState {
	wm := &WaterMutexState{}
	wm.cond = sync.NewCond(&wm.mu)
	return wm
}

func (wm *WaterMutexState) start(ts int) {
	wm.mu.Lock()
	wm.requesting = true
	wm.requestTS = ts
	wm.mu.Unlock()
}

func (wm *WaterMutexState) finish() {
	wm.mu.Lock()
	wm.requesting = false
	wm.requestTS = 0
	wm.cond.Broadcast()
	wm.mu.Unlock()
}

func (wm *WaterMutexState) waitForTurn(peerTS int, peerID, selfID string) {
	wm.mu.Lock()
	localTS := wm.requestTS
	deferred := false
	for wm.requesting && lamportLess(wm.requestTS, selfID, peerTS, peerID) {
		if !deferred {
			fmt.Printf("[%s] [Truck %s] Defer water grant to %s (self_ts=%d, peer_ts=%d)\n",
				stampNow(), selfID, peerID, localTS, peerTS)
			deferred = true
		}
		wm.cond.Wait()
	}
	fmt.Printf("[%s] [Truck %s] Grant water to %s (self_ts=%d, peer_ts=%d)\n",
		stampNow(), selfID, peerID, localTS, peerTS)
	wm.mu.Unlock()
}

func (wm *WaterMutexState) timestamp() int {
	wm.mu.Lock()
	defer wm.mu.Unlock()
	return wm.requestTS
}

func lamportLess(ts1 int, id1 string, ts2 int, id2 string) bool {
	if ts1 != ts2 {
		return ts1 < ts2
	}
	return id1 < id2
}

type FireState struct {
	X         int
	Y         int
	Intensity int

	LastSeen time.Time

	ClaimedBy string
	ClaimCost int
	ClaimTS   int
	ClaimedAt time.Time

	MyCost int
}

const (
	fireInfoTTL   = 10 * time.Second
	fireClaimTTL  = 20 * time.Second
	fireScanDelay = 3 * time.Second
)

/*
================================= Task 0 Template =================================
This is a very basic, minimal template that aims to provide a starting point for
Task 0 of the firetruck simulation system.

In later tasks, you will need to extend the simulation system to include:
- Distributed messaging (Task 1)
- Coordination and logical clocks (Task 2)
- Decentralized strategies (Task 3)

Currently implemented features:
- 20x20 grid with fires and trucks
- Fires can randomly ignite and spread/intensify
- Firetrucks can be created and registered on the grid
  (extend to include the rest of the firetruck functionalities)
- Central manager for handling messages
  (this should be changed later when implementing distributed communication)
====================================================================================
*/

// New type "Message" for truck-manager communication

// ----------------------- Central manager -----------------------
// Create manager
func createManager(size int, waterCapacity int, refillRate int) map[string]interface{} {
	grid := make([][]map[string]interface{}, size)
	for i := 0; i < size; i++ {
		grid[i] = make([]map[string]interface{}, size)
		for j := 0; j < size; j++ {
			grid[i][j] = map[string]interface{}{
				"fire":      false,
				"intensity": 0,
				"truck":     "",
				"location":  flatFireName(i, j),
			}
		}
	}
	return map[string]interface{}{
		"grid":   grid,
		"water":  waterCapacity,
		"max":    waterCapacity,
		"refill": refillRate,
		"inbox":  make(chan Message, 100), // This is where the messages from the trucks are sent to. It can hold up to 100 messages
		"clock":  NewLamportClock(),
		"locker": &sync.RWMutex{},
	}
}

// Run the central manager, which is responsible for what's happening on the grid
func runManager(manager map[string]interface{}, stopAfter int, tick time.Duration, done chan struct{}) {
	inbox := manager["inbox"].(chan Message)
	size := len(manager["grid"].([][]map[string]interface{}))
	mu := managerMutex(manager)

	timestep := 0
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		timestep++
		mu.Lock()
		drainMessages(manager)

		if timestep%2 == 0 {
			spreadFires(manager)
		}
		if rng.Float32() < 0.5 {
			addFire(manager, rng.Intn(size), rng.Intn(size))
		}
		refillWater(manager)

		fmt.Printf("\n--- TIMESTEP %d ---\n", timestep)
		display(manager)
		mu.Unlock()

		// Wait until next tick
		waitUntil := time.Now().Add(tick)
		for time.Now().Before(waitUntil) {
			select {
			case msg := <-inbox:
				handleMessage(manager, msg)
			default:
				time.Sleep(10 * time.Millisecond)
			}
		}

		if stopAfter > 0 && timestep >= stopAfter {
			fmt.Println("Simulation ended after", stopAfter, "timesteps.")
			close(done) // Signal to main that manager is finished (all timesteps completed)
			return
		}
	}
}

// Drain messages from the manager's inbox; it is called at the start of each timestep (tick),
// meaning that the manager processes all pending truck requests at the start of each timestep (tick)
func drainMessages(manager map[string]interface{}) {
	inbox := manager["inbox"].(chan Message)
	for {
		select {
		case msg := <-inbox:
			handleMessageLocked(manager, msg)
		default:
			return
		}
	}
}

func managerClock(manager map[string]interface{}) *LamportClock {
	if clk, ok := manager["clock"].(*LamportClock); ok && clk != nil {
		return clk
	}
	clk := NewLamportClock()
	manager["clock"] = clk
	return clk
}

func managerMutex(manager map[string]interface{}) *sync.RWMutex {
	if mu, ok := manager["locker"].(*sync.RWMutex); ok && mu != nil {
		return mu
	}
	mu := &sync.RWMutex{}
	manager["locker"] = mu
	return mu
}

func sendManagerReply(manager map[string]interface{}, reply chan Message, resp Message) {
	if reply == nil {
		return
	}
	managerClock(manager).Stamp(&resp)
	reply <- resp
}

func handleMessage(manager map[string]interface{}, msg Message) {
	mu := managerMutex(manager)
	mu.Lock()
	defer mu.Unlock()
	handleMessageLocked(manager, msg)
}

func handleMessageLocked(manager map[string]interface{}, msg Message) {
	managerClock(manager).Receive(msg.TS)

	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)

	switch msg.Type {

	case "register":
		x, y := *msg.X, *msg.Y
		id := flatTruckName(msg.From)

		if !inBounds(size, x, y) {
			sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "out of bounds"})
			return
		}
		if grid[x][y]["truck"].(string) != "" {
			sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "cell occupied"})
			return
		}

		grid[x][y]["truck"] = id
		sendManagerReply(manager, msg.Reply, Message{OK: pbool(true), Info: "registered", Resource: id})
		return

	case "nearest_fire":
		sx, sy := *msg.X, *msg.Y
		nx, ny, ok := nearestFireFrom(grid, sx, sy)
		if ok {
			loc := grid[nx][ny]["location"].(string)
			sendManagerReply(manager, msg.Reply, Message{
				Type:     "nearest_fire",
				OK:       pbool(true),
				X:        pint(nx),
				Y:        pint(ny),
				Info:     loc,
				Resource: loc,
			})
		} else {
			sendManagerReply(manager, msg.Reply, Message{Type: "nearest_fire", OK: pbool(false), Info: "no fire"})
		}
		return

	case "move":
		id := flatTruckName(msg.From)
		sx, sy := *msg.X, *msg.Y
		nx, ny := *msg.NX, *msg.NY

		if !inBounds(size, nx, ny) {
			sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "out of bounds"})
			return
		}
		if grid[sx][sy]["truck"].(string) != id {
			sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "not at source"})
			return
		}
		if grid[nx][ny]["truck"].(string) != "" {
			sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "blocked"})
			return
		}

		grid[sx][sy]["truck"] = ""
		grid[nx][ny]["truck"] = id
		location := grid[nx][ny]["location"].(string)
		sendManagerReply(manager, msg.Reply, Message{
			OK:       pbool(true),
			X:        pint(nx),
			Y:        pint(ny),
			Info:     "moved",
			Resource: location,
		})
		return

	case "extinguish":
		id := flatTruckName(msg.From)
		x, y := *msg.X, *msg.Y
		truckWater := *msg.TruckWater

		if !inBounds(size, x, y) || grid[x][y]["truck"].(string) != id {
			sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "not at location"})
			return
		}
		if !grid[x][y]["fire"].(bool) {
			sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "no fire"})
			return
		}
		if truckWater <= 0 {
			sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "no truck water"})
			return
		}

		intensity := grid[x][y]["intensity"].(int)
		remove := 3
		if remove > intensity {
			remove = intensity
		}
		if remove > truckWater {
			remove = truckWater
		}
		intensity -= remove
		if intensity <= 0 {
			grid[x][y]["fire"] = false
			intensity = 0
		}
		grid[x][y]["intensity"] = intensity

		location := grid[x][y]["location"].(string)
		sendManagerReply(manager, msg.Reply, Message{
			OK:        pbool(true),
			Spent:     pint(remove),
			Intensity: pint(intensity),
			Resource:  location,
		})
		return

	case "refill_truck":
		id := flatTruckName(msg.From)
		x, y := *msg.X, *msg.Y
		need := *msg.Need

		if !inBounds(size, x, y) || grid[x][y]["truck"].(string) != id {
			sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "not at location"})
			return
		}

		avail := manager["water"].(int)
		if avail <= 0 || need <= 0 {
			sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "no global water"})
			return
		}
		if need > avail {
			need = avail
		}
		manager["water"] = avail - need

		location := grid[x][y]["location"].(string)
		sendManagerReply(manager, msg.Reply, Message{
			OK:       pbool(true),
			Granted:  pint(need),
			Resource: location,
		})
		return

	default:
		sendManagerReply(manager, msg.Reply, Message{OK: pbool(false), Info: "unknown"})
	}
}

func nearestFireFrom(grid [][]map[string]interface{}, sx, sy int) (int, int, bool) {
	size := len(grid)
	bestX, bestY := -1, -1
	bestDist := int(^uint(0) >> 1)
	found := false

	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			if grid[i][j]["fire"].(bool) {
				d := abs(i-sx) + abs(j-sy)
				if d < bestDist {
					bestDist = d
					bestX, bestY = i, j
					found = true
				}
			}
		}
	}
	return bestX, bestY, found
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func stepToward(sx, sy, tx, ty int) (nx, ny int) {
	nx, ny = sx, sy
	dx, dy := tx-sx, ty-sy
	if abs(dx) >= abs(dy) {
		if dx > 0 {
			nx++
		} else if dx < 0 {
			nx--
		}
	} else {
		if dy > 0 {
			ny++
		} else if dy < 0 {
			ny--
		}
	}
	return
}

// ------------------- Manager helper functions -------------------
func updateCaptainAssignment(truck map[string]interface{}, bus *TruckBus) {
	current := bus.CurrentCaptain()
	prev, _ := truck["captain"].(string)
	if current == prev {
		return
	}
	truck["captain"] = current
	id := truck["id"].(string)
	if current == "" {
		fmt.Printf("[Truck %s] Waiting for captain election\n", id)
		return
	}
	if bus.IsCaptain() {
		fmt.Printf("[Truck %s] Acting as truck captain\n", id)
	} else {
		fmt.Printf("[Truck %s] Truck %s elected captain\n", id, current)
	}
}

func handlePeerMessage(truck map[string]interface{}, msg Message) {
	truckClock(truck).Receive(msg.TS)
	selfID := truck["id"].(string)
	if msg.From == "" || msg.From == selfID {
		return
	}
	switch msg.Type {
	case "status_update":
		pos := "(unknown)"
		if msg.X != nil && msg.Y != nil {
			pos = fmt.Sprintf("(%d,%d)", *msg.X, *msg.Y)
		}
		water := ""
		if msg.TruckWater != nil {
			water = fmt.Sprintf(", water=%d", *msg.TruckWater)
		}
		note := msg.Info
		if note == "" {
			note = "status"
		}
		resource := msg.Resource
		if resource == "" && msg.X != nil && msg.Y != nil {
			resource = flatFireName(*msg.X, *msg.Y)
		}
		resSuffix := ""
		if resource != "" {
			resSuffix = fmt.Sprintf(" {%s}", resource)
		}
		fmt.Printf("[Truck %s] Peer %s %s%s [%s]%s\n", selfID, msg.From, pos, water, note, resSuffix)
	default:
		fmt.Printf("[Truck %s] Peer message %s from %s\n", selfID, msg.Type, msg.From)
	}
}

func handleBroadcastMessage(truck map[string]interface{}, msg Message) {
	truckClock(truck).Receive(msg.TS)
	if msg.From == "" {
		return
	}
	selfID := truck["id"].(string)
	fireID := msg.Resource
	if fireID == "" && msg.X != nil && msg.Y != nil {
		fireID = flatFireName(*msg.X, *msg.Y)
	}
	switch msg.Type {
	case "fire_spotted":
		if msg.X == nil || msg.Y == nil || msg.From == selfID {
			return
		}
		intensity := -1
		if msg.Intensity != nil {
			intensity = *msg.Intensity
		}
		fs, fresh := observeFire(truck, fireID, *msg.X, *msg.Y, intensity)
		if fresh {
			fmt.Printf("[Truck %s] Fire spotted by %s at %s (%d,%d)\n",
				selfID, msg.From, fireID, fs.X, fs.Y)
		}
	case "fire_claim":
		if fireID == "" || msg.From == selfID {
			return
		}
		if msg.X != nil && msg.Y != nil {
			observeFire(truck, fireID, *msg.X, *msg.Y, -1)
		}
		cost := 0
		if msg.Need != nil {
			cost = *msg.Need
		}
		fs := ensureFireState(truck, fireID)
		if fs.ClaimedBy == msg.From {
			if msg.TS != nil {
				fs.ClaimTS = *msg.TS
			}
			if cost > 0 {
				fs.ClaimCost = cost
			}
			fs.ClaimedAt = time.Now()
			return
		}
		curTarget := currentTarget(truck)
		if claimBeats(fs.ClaimCost, fs.ClaimedBy, cost, msg.From) {
			adoptClaim(fs, msg.From, cost, msg.TS)
			if curTarget == fireID && fs.ClaimedBy != selfID {
				setCurrentTarget(truck, "")
				forceRescan(truck)
				fmt.Printf("[Truck %s] Yielded fire %s to %s (cost=%d)\n",
					selfID, fireID, msg.From, cost)
				// Trigger immediate rescan to redirect to another unclaimed fire
				truck["needsRescan"] = true
			} else {
				fmt.Printf("[Truck %s] Fire %s claimed by %s (cost=%d)\n",
					selfID, fireID, msg.From, cost)
			}
		}
	case "fire_release":
		if fireID == "" || msg.From == "" {
			return
		}
		if fs, ok := fireRegistry(truck)[fireID]; ok {
			releaseClaim(fs)
		}
		if currentTarget(truck) == fireID {
			setCurrentTarget(truck, "")
			forceRescan(truck)
		}
		fmt.Printf("[Truck %s] Fire %s released by %s\n", selfID, fireID, msg.From)
	default:
		fmt.Printf("[Truck %s] Broadcast %s from %s\n", selfID, msg.Type, msg.From)
	}
}

// TODO for Task 0: add/modify/expand as needed

func inBounds(size, x, y int) bool {
	return x >= 0 && x < size && y >= 0 && y < size
}

// Add new fire to the grid
func addFire(manager map[string]interface{}, x, y int) {
	grid := manager["grid"].([][]map[string]interface{})
	if !grid[x][y]["fire"].(bool) {
		grid[x][y]["fire"] = true
		grid[x][y]["intensity"] = rng.Intn(3) + 1
	}
}

// Spread fires, increases existing fires' intensity (basic implementation)
func spreadFires(manager map[string]interface{}) {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)
	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			cell := grid[i][j]
			if cell["fire"].(bool) {
				cell["intensity"] = cell["intensity"].(int) + 1
				if cell["intensity"].(int) > 10 { // Arbitrarily cap fire intensity at 10
					cell["intensity"] = 10
				}
			}
		}
	}
}

// Refill the global shared water supply
func refillWater(manager map[string]interface{}) {
	w := manager["water"].(int) + manager["refill"].(int)
	if w > manager["max"].(int) {
		w = manager["max"].(int)
	}
	manager["water"] = w
}

// ----------------------- Display -----------------------
func display(manager map[string]interface{}) {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)
	fmt.Print("   ")
	for j := 0; j < size; j++ {
		fmt.Printf("%3d", j) // Column numbers
	}
	fmt.Println()
	for i := 0; i < size; i++ {
		fmt.Printf("%2d ", i) // Row numbers
		for j := 0; j < size; j++ {
			cell := grid[i][j]
			switch {
			case cell["truck"].(string) != "" && cell["fire"].(bool):
				fmt.Print("💧") // Truck fighting fire
			case cell["truck"].(string) != "":
				fmt.Print("🚛") // Truck
			case cell["fire"].(bool):
				intensity := cell["intensity"].(int)
				// Fire intensity symbols and levels with arbitrary values
				switch {
				case intensity >= 8:
					fmt.Print("🔥")
				case intensity >= 5:
					fmt.Print("🟥")
				case intensity >= 3:
					fmt.Print("🟧")
				default:
					fmt.Print("🟨")
				}
			default:
				fmt.Print("🌲") // Forest cell with no fire or truck on it
			}
			fmt.Print(" ")
		}
		fmt.Println()
	}
	// Print the current water supply level
	fmt.Println("Water:", manager["water"], "/", manager["max"])
}

// ----------------------- Firetruck -----------------------
// Add/modify/expand as needed

// Create a firetruck - note: this creates a local truck but does not register it to the grid
func createTruck(id string, x, y int, peerIDs []string, natsURL string) map[string]interface{} {
	flatID := flatTruckName(id)
	flatPeers := make([]string, len(peerIDs))
	for i, pid := range peerIDs {
		flatPeers[i] = flatTruckName(pid)
	}

	tc, err := dialManager(managerTCPAddr) // TCP client
	if err != nil {
		panic(err)
	}
	clock := NewLamportClock()
	bus, err := newTruckBus(flatID, flatPeers, natsURL, clock)
	if err != nil {
		panic(err)
	}
	return map[string]interface{}{
		"id":          flatID,
		"rawID":       id,
		"x":           x,
		"y":           y,
		"conn":        tc,
		"water":       5,
		"maxWater":    10,
		"bus":         bus,
		"captain":     bus.CurrentCaptain(),
		"clock":       clock,
		"waterMutex":  newWaterMutexState(),
		"fires":       make(map[string]*FireState),
		"target":      "",
		"nextScan":    time.Now(),
		"needsRescan": false,
	}
}

func truckClock(truck map[string]interface{}) *LamportClock {
	if clk, ok := truck["clock"].(*LamportClock); ok && clk != nil {
		return clk
	}
	clk := NewLamportClock()
	truck["clock"] = clk
	return clk
}

func waterMutex(truck map[string]interface{}) *WaterMutexState {
	if wm, ok := truck["waterMutex"].(*WaterMutexState); ok && wm != nil {
		return wm
	}
	wm := newWaterMutexState()
	truck["waterMutex"] = wm
	return wm
}

func fireRegistry(truck map[string]interface{}) map[string]*FireState {
	if fires, ok := truck["fires"].(map[string]*FireState); ok && fires != nil {
		return fires
	}
	fires := make(map[string]*FireState)
	truck["fires"] = fires
	return fires
}

func ensureFireState(truck map[string]interface{}, fireID string) *FireState {
	fires := fireRegistry(truck)
	if fs, ok := fires[fireID]; ok && fs != nil {
		return fs
	}
	fs := &FireState{}
	fires[fireID] = fs
	return fs
}

func getFireState(truck map[string]interface{}, fireID string) (*FireState, bool) {
	fs := ensureFireState(truck, fireID)
	if fs.X == 0 && fs.Y == 0 && fs.LastSeen.IsZero() {
		return fs, false
	}
	return fs, true
}

func currentTarget(truck map[string]interface{}) string {
	if tgt, ok := truck["target"].(string); ok {
		return tgt
	}
	return ""
}

func setCurrentTarget(truck map[string]interface{}, fireID string) {
	truck["target"] = fireID
}

func resetCurrentTarget(truck map[string]interface{}, fireID string) {
	if fireID == "" {
		return
	}
	if tgt := currentTarget(truck); tgt == fireID {
		truck["target"] = ""
	}
}

func nextScanDue(truck map[string]interface{}) time.Time {
	if ts, ok := truck["nextScan"].(time.Time); ok {
		return ts
	}
	return time.Time{}
}

func scheduleNextScan(truck map[string]interface{}, delay time.Duration) {
	truck["nextScan"] = time.Now().Add(delay)
}

func forceRescan(truck map[string]interface{}) {
	truck["nextScan"] = time.Now()
}

func observeFire(truck map[string]interface{}, fireID string, x, y int, intensity int) (*FireState, bool) {
	fs := ensureFireState(truck, fireID)
	prevSeen := fs.LastSeen
	fs.X = x
	fs.Y = y
	if intensity >= 0 {
		fs.Intensity = intensity
	}
	fs.LastSeen = time.Now()
	fresh := prevSeen.IsZero() || time.Since(prevSeen) > 500*time.Millisecond
	return fs, fresh
}

func manhattanDistance(x1, y1, x2, y2 int) int {
	dx := x1 - x2
	if dx < 0 {
		dx = -dx
	}
	dy := y1 - y2
	if dy < 0 {
		dy = -dy
	}
	return dx + dy
}

func computeFireCost(truck map[string]interface{}, fs *FireState) int {
	curX := truck["x"].(int)
	curY := truck["y"].(int)
	cost := manhattanDistance(curX, curY, fs.X, fs.Y)
	if water, ok := truck["water"].(int); ok && water == 0 {
		cost += 5
	}
	return cost
}

func claimBeats(existingCost int, existingID string, incomingCost int, incomingID string) bool {
	if existingID == "" {
		return true
	}
	if incomingCost != existingCost {
		return incomingCost < existingCost
	}
	return incomingID < existingID
}

func adoptClaim(fs *FireState, claimant string, cost int, ts *int) {
	fs.ClaimedBy = claimant
	fs.ClaimCost = cost
	fs.ClaimedAt = time.Now()
	if ts != nil {
		fs.ClaimTS = *ts
	}
}

func releaseClaim(fs *FireState) {
	fs.ClaimedBy = ""
	fs.ClaimCost = 0
	fs.ClaimTS = 0
	fs.ClaimedAt = time.Time{}
}

func pruneStaleFires(truck map[string]interface{}) {
	selfID := truck["id"].(string)
	fires := fireRegistry(truck)
	for id, fs := range fires {
		if !fs.ClaimedAt.IsZero() && time.Since(fs.ClaimedAt) > fireClaimTTL {
			if fs.ClaimedBy == selfID {
				broadcastFireRelease(truck, id, fs.X, fs.Y)
			}
			releaseClaim(fs)
		}
		if time.Since(fs.LastSeen) > fireInfoTTL {
			delete(fires, id)
		}
	}
}

func broadcastFireSpot(truck map[string]interface{}, fs *FireState) {
	bus, _ := truck["bus"].(*TruckBus)
	if bus == nil || !bus.Enabled() {
		return
	}
	msg := Message{
		Type:     "fire_spotted",
		From:     truck["id"].(string),
		Resource: flatFireName(fs.X, fs.Y),
		X:        pint(fs.X),
		Y:        pint(fs.Y),
	}
	if fs.Intensity > 0 {
		msg.Intensity = pint(fs.Intensity)
	}
	bus.Broadcast(msg)
}

func pickBestFire(truck map[string]interface{}) (string, *FireState, int) {
	selfID := truck["id"].(string)
	fires := fireRegistry(truck)
	bestID := ""
	var bestFS *FireState
	bestCost := int(^uint(0) >> 1)
	for id, fs := range fires {
		if time.Since(fs.LastSeen) > fireInfoTTL {
			continue
		}
		if fs.ClaimedBy != "" && fs.ClaimedBy != selfID {
			if !fs.ClaimedAt.IsZero() && time.Since(fs.ClaimedAt) <= fireClaimTTL {
				continue
			}
		}
		cost := computeFireCost(truck, fs)
		fs.MyCost = cost
		if bestFS == nil || cost < bestCost || (cost == bestCost && id < bestID) {
			bestID = id
			bestFS = fs
			bestCost = cost
		}
	}
	return bestID, bestFS, bestCost
}

func broadcastFireClaim(truck map[string]interface{}, fs *FireState, cost int) {
	bus, _ := truck["bus"].(*TruckBus)
	if bus == nil || !bus.Enabled() {
		return
	}
	msg := Message{
		Type:     "fire_claim",
		From:     truck["id"].(string),
		Resource: flatFireName(fs.X, fs.Y),
		X:        pint(fs.X),
		Y:        pint(fs.Y),
		Need:     pint(cost),
	}
	if fs.Intensity > 0 {
		msg.Intensity = pint(fs.Intensity)
	}
	bus.Broadcast(msg)
}

func broadcastFireRelease(truck map[string]interface{}, fireID string, x, y int) {
	bus, _ := truck["bus"].(*TruckBus)
	if bus == nil || !bus.Enabled() {
		return
	}
	bus.Broadcast(Message{
		Type:     "fire_release",
		From:     truck["id"].(string),
		Resource: fireID,
		X:        pint(x),
		Y:        pint(y),
	})
}

func managerRequest(truck map[string]interface{}, msg Message) (Message, error) {
	clk := truckClock(truck)
	clk.Stamp(&msg)
	tc := truck["conn"].(*TruckConn)
	resp, err := tc.req(msg)
	if err != nil {
		return Message{}, err
	}
	clk.Receive(resp.TS)
	return resp, nil
}

func acquireWaterMutex(truck map[string]interface{}, need int) bool {
	wm := waterMutex(truck)
	id := truck["id"].(string)
	ts := truckClock(truck).Send()
	wm.start(ts)
	fmt.Printf("[%s] [Truck %s] Requesting shared water (ts=%d, need=%d)\n",
		stampNow(), id, ts, need)

	bus, _ := truck["bus"].(*TruckBus)
	if bus == nil || !bus.Enabled() {
		fmt.Printf("[%s] [Truck %s] No peers online; water access granted immediately (ts=%d)\n",
			stampNow(), id, ts)
		return true
	}
	needVal := need
	if needVal < 0 {
		needVal = 0
	}
	for _, peer := range bus.PeerIDs() {
		if peer == id {
			continue
		}
		if !bus.SeenRecently(peer, peerTTL) {
			fmt.Printf("[%s] [Truck %s] Skip water request to %s (peer offline)\n",
				stampNow(), id, peer)
			continue
		}
		fmt.Printf("[%s] [Truck %s] Send water request to %s (req_ts=%d, need=%d)\n",
			stampNow(), id, peer, ts, needVal)
		req := Message{
			Type: "water_request",
			From: id,
		}
		if needVal > 0 {
			req.Need = pint(needVal)
		}
		req.TS = pint(ts)
		resp, err := bus.RequestTo(peer, req, 5*time.Second)
		if err != nil {
			fmt.Printf("[%s] [Truck %s] Water request to %s denied (transport error, ts=%d): %v\n",
				stampNow(), id, peer, ts, err)
			bus.MarkOffline(peer)
			continue
		}
		if resp.OK != nil && !*resp.OK {
			fmt.Printf("[%s] [Truck %s] Water request to %s denied (ts=%d): %s\n",
				stampNow(), id, peer, ts, resp.Info)
			wm.finish()
			return false
		}
		fmt.Printf("[%s] [Truck %s] Water request accepted by %s (req_ts=%d)\n",
			stampNow(), id, peer, ts)
	}
	return true
}

func releaseWaterMutex(truck map[string]interface{}) {
	wm := waterMutex(truck)
	id := truck["id"].(string)
	prevTS := wm.timestamp()
	fmt.Printf("[%s] [Truck %s] Releasing shared water (ts=%d)\n",
		stampNow(), id, prevTS)
	wm.finish()
}

// Register a firetruck to the grid via the manager
func registerTruck(truck map[string]interface{}) {
	_, err := managerRequest(truck, Message{
		Type:     "register",
		From:     truck["id"].(string),
		X:        pint(truck["x"].(int)),
		Y:        pint(truck["y"].(int)),
		Resource: truck["id"].(string),
	})
	if err != nil {
		panic(err)
	}
}

// Skeleton of firetruck actions
func truckLoop(truck map[string]interface{}) {
	bus := truck["bus"].(*TruckBus)
	bus.SetRequestHandler(func(msg Message) Message {
		switch msg.Type {
		case "water_request":
			selfID := truck["id"].(string)
			peerTS := 0
			if msg.TS != nil {
				peerTS = *msg.TS
			}
			need := 0
			if msg.Need != nil {
				need = *msg.Need
			}
			fmt.Printf("[%s] [Truck %s] Incoming water request from %s (peer_ts=%d, need=%d)\n",
				stampNow(), selfID, msg.From, peerTS, need)
			if msg.From != "" {
				waterMutex(truck).waitForTurn(peerTS, msg.From, selfID)
			}
			fmt.Printf("[%s] [Truck %s] Accepted water request from %s (peer_ts=%d)\n",
				stampNow(), selfID, msg.From, peerTS)
			return Message{Type: "water_reply", OK: pbool(true), Info: "granted"}
		default:
			return Message{Type: msg.Type + "_resp", OK: pbool(false), Info: "unsupported"}
		}
	})
	statusFeed := bus.StatusFeed()
	broadcastFeed := bus.BroadcastFeed()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	drainPeers := func() {
		for {
			select {
			case msg, ok := <-statusFeed:
				if !ok {
					return
				}
				handlePeerMessage(truck, msg)
			case bmsg, ok := <-broadcastFeed:
				if !ok {
					return
				}
				handleBroadcastMessage(truck, bmsg)
			default:
				return
			}
		}
	}

	reportStatus := func(note string) {
		resource := truck["id"].(string)
		if tgt := currentTarget(truck); tgt != "" {
			resource = tgt
		}
		bus.PublishStatus(Message{
			Info:       note,
			X:          pint(truck["x"].(int)),
			Y:          pint(truck["y"].(int)),
			TruckWater: pint(truck["water"].(int)),
			Resource:   resource,
		})
	}

	reportStatus("online")
	updateCaptainAssignment(truck, bus)

	for range ticker.C {
		drainPeers()
		updateCaptainAssignment(truck, bus)
		pruneStaleFires(truck)

		selfID := truck["id"].(string)

		if time.Now().After(nextScanDue(truck)) {
			scheduleNextScan(truck, fireScanDelay)
			resp, err := managerRequest(truck, Message{
				Type: "nearest_fire",
				From: selfID,
				X:    pint(truck["x"].(int)),
				Y:    pint(truck["y"].(int)),
			})
			if err != nil {
				fmt.Printf("[Truck %s] nearest_fire error: %v\n", selfID, err)
			} else if resp.OK != nil && *resp.OK {
				targetX := *resp.X
				targetY := *resp.Y
				fireID := flatFireName(targetX, targetY)
				fs, fresh := observeFire(truck, fireID, targetX, targetY, -1)
				if fresh {
					broadcastFireSpot(truck, fs)
				}
			}
		}

		// Check if we need to redirect to another fire
		if needsRescan, ok := truck["needsRescan"].(bool); ok && needsRescan {
			truck["needsRescan"] = false
			newID, fs, cost := pickBestFire(truck)
			if newID != "" {
				setCurrentTarget(truck, newID)
				adoptClaim(fs, selfID, cost, nil)
				broadcastFireClaim(truck, fs, cost)
				fmt.Printf("[Truck %s] Redirecting to fire %s (cost=%d)\n", selfID, newID, cost)
				reportStatus("redirecting")
				continue
			} else {
				fmt.Printf("[Truck %s] No alternative fire found, going idle\n", selfID)
				reportStatus("idle")
				continue
			}
		}

		targetID := currentTarget(truck)
		fires := fireRegistry(truck)
		target := fires[targetID]
		if targetID != "" {
			if target == nil {
				setCurrentTarget(truck, "")
				forceRescan(truck)
				targetID = ""
			} else if time.Since(target.LastSeen) > fireInfoTTL {
				if target.ClaimedBy == selfID {
					broadcastFireRelease(truck, targetID, target.X, target.Y)
					releaseClaim(target)
				}
				delete(fires, targetID)
				setCurrentTarget(truck, "")
				forceRescan(truck)
				target = nil
				targetID = ""
			}
		}

		justClaimed := false

		if target == nil {
			newID, fs, cost := pickBestFire(truck)
			if newID != "" {
				targetID = newID
				target = fs
				setCurrentTarget(truck, targetID)
				adoptClaim(target, selfID, cost, nil)
				broadcastFireClaim(truck, target, cost)
				fmt.Printf("[Truck %s] Claiming fire %s (cost=%d)\n", selfID, targetID, cost)
				reportStatus("claiming")
				justClaimed = true
			}
		} else if target.ClaimedBy == "" || target.ClaimedBy == selfID {
			cost := computeFireCost(truck, target)
			target.MyCost = cost
			if time.Since(target.ClaimedAt) > 2*time.Second {
				adoptClaim(target, selfID, cost, nil)
				broadcastFireClaim(truck, target, cost)
			}
		} else if target.ClaimedBy != selfID {
			setCurrentTarget(truck, "")
			target = nil
			targetID = ""
		}

		if justClaimed {
			continue
		}

		if target == nil {
			reportStatus("idle")
			continue
		}

		curX := truck["x"].(int)
		curY := truck["y"].(int)
		targetX := target.X
		targetY := target.Y
		targetName := targetID

		if target.ClaimedBy != selfID {
			fmt.Printf("[Truck %s] Yielding fire %s to %s\n", selfID, targetName, target.ClaimedBy)
			setCurrentTarget(truck, "")
			forceRescan(truck)
			reportStatus("yielded_claim")
			continue
		}

		if curX == targetX && curY == targetY {
			w := truck["water"].(int)
			if w == 0 {
				want := truck["maxWater"].(int)
				need := want - w
				if need <= 0 {
					need = want
				}
				if !acquireWaterMutex(truck, need) {
					fmt.Printf("[%s] [Truck %s] Denied water refill (mutex acquisition failed)\n",
						stampNow(), selfID)
					reportStatus("water_mutex_failed")
					time.Sleep(200 * time.Millisecond)
					continue
				}
				refillResp, err := managerRequest(truck, Message{
					Type:     "refill_truck",
					From:     selfID,
					X:        pint(curX),
					Y:        pint(curY),
					Need:     pint(need),
					Resource: targetName,
				})
				releaseWaterMutex(truck)
				if err != nil {
					fmt.Printf("[Truck %s] refill error at %s: %v\n", selfID, targetName, err)
					reportStatus("refill_error")
					continue
				}
				if refillResp.OK != nil && *refillResp.OK {
					granted := *refillResp.Granted
					maxW := truck["maxWater"].(int)
					cur := truck["water"].(int)
					newW := cur + granted
					if newW > maxW {
						newW = maxW
					}
					truck["water"] = newW
					fmt.Printf("[Truck %s] Refilled %d units at %s. Tank: %d/%d\n",
						selfID, newW-cur, targetName, truck["water"], maxW)
					reportStatus("refilled")
				} else {
					fmt.Printf("[Truck %s] Refill failed at %s: %v\n", selfID, targetName, refillResp.Info)
					reportStatus("refill_denied")
				}
				continue
			}

			extResp, err := managerRequest(truck, Message{
				Type:       "extinguish",
				From:       selfID,
				X:          pint(curX),
				Y:          pint(curY),
				TruckWater: pint(w),
				Resource:   targetName,
			})
			if err != nil {
				fmt.Printf("[Truck %s] extinguish error at %s: %v\n", selfID, targetName, err)
				reportStatus("extinguish_error")
				continue
			}
			if extResp.OK != nil && *extResp.OK {
				spent := *extResp.Spent
				truck["water"] = truck["water"].(int) - spent
				intensityStr := "unknown"
				if extResp.Intensity != nil {
					target.Intensity = *extResp.Intensity
					intensityStr = fmt.Sprintf("%d", *extResp.Intensity)
				}
				fmt.Printf("[Truck %s] Extinguishing %s at (%d,%d). Intensity now: %s\n",
					selfID, targetName, curX, curY, intensityStr)
				reportStatus("extinguishing")
				if extResp.Intensity != nil && *extResp.Intensity == 0 {
					broadcastFireRelease(truck, targetName, targetX, targetY)
					releaseClaim(target)
					setCurrentTarget(truck, "")
					delete(fires, targetName)
				}
			} else {
				fmt.Printf("[Truck %s] Extinguish failed at %s: %v\n", selfID, targetName, extResp.Info)
				reportStatus("extinguish_denied")
			}
			continue
		}

		nx, ny := stepToward(curX, curY, targetX, targetY)
		stepName := flatFireName(nx, ny)
		moveResp, err := managerRequest(truck, Message{
			Type:     "move",
			From:     selfID,
			X:        pint(curX),
			Y:        pint(curY),
			NX:       pint(nx),
			NY:       pint(ny),
			Resource: stepName,
		})
		if err != nil {
			fmt.Printf("[Truck %s] move error toward %s: %v\n", selfID, stepName, err)
			reportStatus("move_error")
			continue
		}
		if moveResp.OK != nil && *moveResp.OK {
			truck["x"] = *moveResp.X
			truck["y"] = *moveResp.Y
			destName := moveResp.Resource
			if destName == "" {
				destName = stepName
			}
			fmt.Printf("[Truck %s] Moved to %s (%d,%d)\n", selfID, destName, truck["x"], truck["y"])
			reportStatus(fmt.Sprintf("moving_to_%s", targetName))
		} else {
			destName := moveResp.Resource
			if destName == "" {
				destName = stepName
			}
			fmt.Printf("[Truck %s] Move failed toward %s: %v\n", selfID, destName, moveResp.Info)
			reportStatus("move_denied")
		}
	}
}

// TODO for Task 0: add functionality:
// - decide movement direction DONE
// - send move request DONE
// - send water request DONE
// - communicate DONE
// - extinguish fire DONE

// ----------------------- Main -----------------------
func main() {
	manager := createManager(20, 500, 1)

	// Start TCP server and wait for it to be ready
	go runManagerTCP(manager)
	time.Sleep(500 * time.Millisecond) // Give TCP server time to start listening

	// Create and register trucks BEFORE starting the manager loop
	truckIDs := []string{"T1", "T2"}
	natsURL := nats.DefaultURL
	var trucks []map[string]interface{}
	for _, id := range truckIDs {
		truck := createTruck(id, rng.Intn(20), rng.Intn(20), truckIDs, natsURL)
		registerTruck(truck)
		trucks = append(trucks, truck)
	}

	// Start truck loops before the manager loop
	for _, truck := range trucks {
		go truckLoop(truck)
	}

	// Now start the manager loop after everything else is ready
	done := make(chan struct{})
	go runManager(manager, 50, 2*time.Second, done)

	<-done
}
