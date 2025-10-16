package main

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/nats-io/nats.go"
)

var rng = rand.New(rand.NewSource(time.Now().UnixNano()))

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
			}
		}
	}
	return map[string]interface{}{
		"grid":   grid,
		"water":  waterCapacity,
		"max":    waterCapacity,
		"refill": refillRate,
		"inbox":  make(chan Message, 100), // This is where the messages from the trucks are sent to. It can hold up to 100 messages
	}
}

// Run the central manager, which is responsible for what's happening on the grid
func runManager(manager map[string]interface{}, stopAfter int, tick time.Duration, done chan struct{}) {
	inbox := manager["inbox"].(chan Message)
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)

	timestep := 0
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		timestep++
		// Handle messages from trucks
		drainMessages(manager)

		// Spread fires every 2 timesteps
		if timestep%2 == 0 {
			spreadFires(manager)
		}

		// Random new fire
		if rng.Float32() < 0.5 {
			addFire(manager, rng.Intn(size), rng.Intn(size))
		}

		// Refill global water supply so that it doesn't just run out
		refillWater(manager)

		// Display grid
		fmt.Printf("\n--- TIMESTEP %d ---\n", timestep)
		display(manager)

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
			handleMessage(manager, msg)
		default:
			return
		}
	}
}

// Handle messages
func handleMessage(manager map[string]interface{}, msg Message) {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)

	switch msg.Type {

	case "register":
		x, y := *msg.X, *msg.Y
		id := msg.From

		if !inBounds(size, x, y) {
			if msg.Reply != nil {
				msg.Reply <- Message{OK: pbool(false), Info: "out of bounds"}
			}
			return
		}
		if grid[x][y]["truck"].(string) != "" {
			if msg.Reply != nil {
				msg.Reply <- Message{OK: pbool(false), Info: "cell occupied"}
			}
			return
		}

		grid[x][y]["truck"] = id
		if msg.Reply != nil {
			msg.Reply <- Message{OK: pbool(true), Info: "registered"}
		}
		return

	case "nearest_fire":
		sx, sy := *msg.X, *msg.Y
		nx, ny, ok := nearestFireFrom(grid, sx, sy)
		if msg.Reply != nil {
			if ok {
				msg.Reply <- Message{Type: "nearest_fire", OK: pbool(true), X: pint(nx), Y: pint(ny), Info: "nearest_fire"}
			} else {
				msg.Reply <- Message{Type: "nearest_fire", OK: pbool(false), Info: "no fire"}
			}
		}
		return

	case "move":
		id := msg.From
		sx, sy := *msg.X, *msg.Y
		nx, ny := *msg.NX, *msg.NY

		if !inBounds(size, nx, ny) {
			msg.Reply <- Message{OK: pbool(false), Info: "out of bounds"}
			return
		}
		if grid[sx][sy]["truck"].(string) != id {
			msg.Reply <- Message{OK: pbool(false), Info: "not at source"}
			return
		}
		if grid[nx][ny]["truck"].(string) != "" {
			msg.Reply <- Message{OK: pbool(false), Info: "blocked"}
			return
		}

		grid[sx][sy]["truck"] = ""
		grid[nx][ny]["truck"] = id
		msg.Reply <- Message{OK: pbool(true), X: pint(nx), Y: pint(ny), Info: "moved"}
		return

	case "extinguish":
		id := msg.From
		x, y := *msg.X, *msg.Y
		truckWater := *msg.TruckWater

		if !inBounds(size, x, y) || grid[x][y]["truck"].(string) != id {
			msg.Reply <- Message{OK: pbool(false), Info: "not at location"}
			return
		}
		if !grid[x][y]["fire"].(bool) {
			msg.Reply <- Message{OK: pbool(false), Info: "no fire"}
			return
		}
		if truckWater <= 0 {
			msg.Reply <- Message{OK: pbool(false), Info: "no truck water"}
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

		msg.Reply <- Message{OK: pbool(true), Spent: pint(remove), Intensity: pint(intensity)}
		return

	case "refill_truck":
		id := msg.From
		x, y := *msg.X, *msg.Y
		need := *msg.Need

		if !inBounds(size, x, y) || grid[x][y]["truck"].(string) != id {
			msg.Reply <- Message{OK: pbool(false), Info: "not at location"}
			return
		}

		avail := manager["water"].(int)
		if avail <= 0 || need <= 0 {
			msg.Reply <- Message{OK: pbool(false), Info: "no global water"}
			return
		}
		if need > avail {
			need = avail
		}
		manager["water"] = avail - need

		msg.Reply <- Message{OK: pbool(true), Granted: pint(need)}
		return

	default:
		if msg.Reply != nil {
			msg.Reply <- Message{OK: pbool(false), Info: "unknown"}
		}
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
		fmt.Printf("[Truck %s] Peer %s %s%s [%s]\n", selfID, msg.From, pos, water, note)
	default:
		fmt.Printf("[Truck %s] Peer message %s from %s\n", selfID, msg.Type, msg.From)
	}
}

func handleBroadcastMessage(truck map[string]interface{}, msg Message) {
	if msg.From == "" {
		return
	}
	switch msg.Type {
	case "assignment":
		target := msg.Info
		if target == "" {
			target = "unknown"
		}
		pos := ""
		if msg.X != nil && msg.Y != nil {
			pos = fmt.Sprintf(" -> (%d,%d)", *msg.X, *msg.Y)
		}
		fmt.Printf("[Truck %s] Captain %s broadcast assignment for %s%s\n",
			truck["id"], msg.From, target, pos)
	default:
		fmt.Printf("[Truck %s] Broadcast %s from %s\n", truck["id"], msg.Type, msg.From)
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
	tc, err := dialManager(":9000") // TCP client
	if err != nil {
		panic(err)
	}
	bus, err := newTruckBus(id, peerIDs, natsURL)
	if err != nil {
		panic(err)
	}
	return map[string]interface{}{
		"id":       id,
		"x":        x,
		"y":        y,
		"conn":     tc,
		"water":    5,
		"maxWater": 10,
		"bus":      bus,
		"captain":  bus.CurrentCaptain(),
	}
}

// Register a firetruck to the grid via the manager
func registerTruck(truck map[string]interface{}) {
	tc := truck["conn"].(*TruckConn)
	msg := Message{
		Type: "register",
		From: truck["id"].(string),
		X:    pint(truck["x"].(int)),
		Y:    pint(truck["y"].(int)),
	}
	_, err := tc.req(msg)
	if err != nil {
		panic(err)
	}
}

// Skeleton of firetruck actions
func truckLoop(truck map[string]interface{}) {
	tc := truck["conn"].(*TruckConn)
	bus := truck["bus"].(*TruckBus)
	bus.SetRequestHandler(func(msg Message) Message {
		switch msg.Type {
		case "assignment_proposal":
			if bus.IsCaptain() {
				bus.Broadcast(Message{
					Type: "assignment",
					Info: msg.From,
					X:    msg.X,
					Y:    msg.Y,
				})
				return Message{Type: "assignment_ack", OK: pbool(true), Info: "approved"}
			}
			return Message{Type: "assignment_ack", OK: pbool(false), Info: "not captain"}
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
		bus.PublishStatus(Message{
			Info:       note,
			X:          pint(truck["x"].(int)),
			Y:          pint(truck["y"].(int)),
			TruckWater: pint(truck["water"].(int)),
		})
	}

	reportStatus("online")

	for range ticker.C {
		drainPeers()
		updateCaptainAssignment(truck, bus)

		resp, err := tc.req(Message{
			Type: "nearest_fire",
			From: truck["id"].(string),
			X:    pint(truck["x"].(int)),
			Y:    pint(truck["y"].(int)),
		})
		if err != nil {
			fmt.Printf("[Truck %s] nearest_fire error: %v\n", truck["id"], err)
			reportStatus("nearest_fire_error")
			continue
		}
		if resp.OK != nil && !*resp.OK {
			fmt.Printf("[Truck %s] No fires found.\n", truck["id"])
			reportStatus("idle")
			continue
		}

		targetX := *resp.X
		targetY := *resp.Y

		curX := truck["x"].(int)
		curY := truck["y"].(int)

		if !bus.IsCaptain() {
			ack, err := bus.RequestCaptain(Message{
				Type: "assignment_proposal",
				From: truck["id"].(string),
				X:    pint(targetX),
				Y:    pint(targetY),
			}, 500*time.Millisecond)
			if err != nil {
				fmt.Printf("[Truck %s] captain request error: %v\n", truck["id"], err)
				reportStatus("captain_unreachable")
				continue
			}
			if ack.OK != nil && !*ack.OK {
				fmt.Printf("[Truck %s] Captain denied assignment: %v\n", truck["id"], ack.Info)
				reportStatus("captain_denied")
				time.Sleep(500 * time.Millisecond)
				continue
			}
		}

		if curX == targetX && curY == targetY {
			w := truck["water"].(int)
			if w == 0 {
				want := truck["maxWater"].(int)
				refillResp, err := tc.req(Message{
					Type: "refill_truck",
					From: truck["id"].(string),
					X:    pint(curX),
					Y:    pint(curY),
					Need: pint(want - w),
				})
				if err != nil {
					fmt.Printf("[Truck %s] refill error: %v\n", truck["id"], err)
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
					fmt.Printf("[Truck %s] Refilled %d units. Tank: %d/%d\n",
						truck["id"], newW-cur, truck["water"], maxW)
					reportStatus("refilled")
				} else {
					fmt.Printf("[Truck %s] Refill failed: %v\n", truck["id"], refillResp.Info)
					reportStatus("refill_denied")
				}
				continue
			}

			extResp, err := tc.req(Message{
				Type:       "extinguish",
				From:       truck["id"].(string),
				X:          pint(curX),
				Y:          pint(curY),
				TruckWater: pint(w),
			})
			if err != nil {
				fmt.Printf("[Truck %s] extinguish error: %v\n", truck["id"], err)
				reportStatus("extinguish_error")
				continue
			}
			if extResp.OK != nil && *extResp.OK {
				spent := *extResp.Spent
				truck["water"] = truck["water"].(int) - spent
				fmt.Printf("[Truck %s] Extinguishing at (%d,%d). Intensity now: %v\n",
					truck["id"], curX, curY, *extResp.Intensity)
				reportStatus("extinguishing")
			} else {
				fmt.Printf("[Truck %s] Extinguish failed: %v\n", truck["id"], extResp.Info)
				reportStatus("extinguish_denied")
			}
			continue
		}

		nx, ny := stepToward(curX, curY, targetX, targetY)
		moveResp, err := tc.req(Message{
			Type: "move",
			From: truck["id"].(string),
			X:    pint(curX),
			Y:    pint(curY),
			NX:   pint(nx),
			NY:   pint(ny),
		})
		if err != nil {
			fmt.Printf("[Truck %s] move error: %v\n", truck["id"], err)
			reportStatus("move_error")
			continue
		}
		if moveResp.OK != nil && *moveResp.OK {
			truck["x"] = *moveResp.X
			truck["y"] = *moveResp.Y
			fmt.Printf("[Truck %s] Moved to (%d,%d)\n", truck["id"], truck["x"], truck["y"])
			reportStatus(fmt.Sprintf("moving_to_%d_%d", targetX, targetY))
		} else {
			fmt.Printf("[Truck %s] Move failed: %v\n", truck["id"], moveResp.Info)
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
	go runManagerTCP(manager) // start TCP server
	done := make(chan struct{})
	go runManager(manager, 50, 2*time.Second, done) // tick/spread/display

	// ... fires ...

	truckIDs := []string{"T1", "T2"}
	natsURL := nats.DefaultURL
	var trucks []map[string]interface{}
	for _, id := range truckIDs {
		truck := createTruck(id, rng.Intn(20), rng.Intn(20), truckIDs, natsURL)
		registerTruck(truck)
		trucks = append(trucks, truck)
	}

	for _, truck := range trucks {
		go truckLoop(truck)
	}

	<-done

}
