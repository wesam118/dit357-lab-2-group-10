package main

import (
	"fmt"
	"math/rand"
	"time"
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
  (extend to include the rest of the firetruck functinoalities)
- Central manager for handling messages
  (this should be changed later when implementing distributed communication)
====================================================================================
*/

// New type "Message" for truck-manager communication
type Message map[string]interface{}

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
		if rand.Float32() < 0.5 {
			addFire(manager, rand.Intn(size), rand.Intn(size))
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

// Handle messages (placeholder)
func handleMessage(manager map[string]interface{}, msg Message) {
	grid := manager["grid"].([][]map[string]interface{})
	size := len(grid)
	switch msg["type"] {
	case "register":
		/*
			This example implementation has a central-manager approach.
			For Task 1, you will replace the synchronous channel-based
			truck registration with a network handshake (TCP/NATS).
		*/
		// Truck requests to register at position (x, y) on the grid
		x, y := msg["x"].(int), msg["y"].(int)
		id := msg["from"].(string)

		// Validate grid bounds and cell occupancy
		if !inBounds(size, x, y) {
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{"ok": false, "info": "out of bounds"}
			}
			return
		}
		if grid[x][y]["truck"].(string) != "" {
			if msg["reply"] != nil {
				msg["reply"].(chan Message) <- Message{"ok": false, "info": "cell occupied"}
			}
			return
		}

		// Place truck on grid
		grid[x][y]["truck"] = id

		if msg["reply"] != nil {
			msg["reply"].(chan Message) <- Message{"ok": true, "info": "registered"}
		}
		return

	case "nearest_fire":
		sx, sy := msg["x"].(int), msg["y"].(int)
		nx, ny, ok := nearestFireFrom(grid, sx, sy)
		if msg["reply"] != nil {
			msg["reply"].(chan Message) <- Message{
				"ok":   ok,
				"x":    nx,
				"y":    ny,
				"info": "nearest_fire",
			}
		}
		return

	case "move":
		id := msg["from"].(string)
		sx, sy := msg["x"].(int), msg["y"].(int)
		nx, ny := msg["nx"].(int), msg["ny"].(int)

		if !inBounds(size, nx, ny) {
			msg["reply"].(chan Message) <- Message{"ok": false, "info": "out of bounds"}
			return
		}

		if grid[sx][sy]["truck"].(string) != id {
			msg["reply"].(chan Message) <- Message{"ok": false, "info": "not at source"}
			return
		}
		if grid[nx][ny]["truck"].(string) != "" {
			msg["reply"].(chan Message) <- Message{"ok": false, "info": "blocked"}
			return
		}

		grid[sx][sy]["truck"] = ""
		grid[nx][ny]["truck"] = id

		msg["reply"].(chan Message) <- Message{"ok": true, "x": nx, "y": ny, "info": "moved"}
		return

	case "extinguish":
		id := msg["from"].(string)
		x, y := msg["x"].(int), msg["y"].(int)

		if !inBounds(size, x, y) || grid[x][y]["truck"].(string) != id {
			msg["reply"].(chan Message) <- Message{"ok": false, "info": "not at location"}
			return
		}

		if !grid[x][y]["fire"].(bool) {
			msg["reply"].(chan Message) <- Message{"ok": false, "info": "no fire"}
			return
		}

		intensity := grid[x][y]["intensity"].(int)
		if intensity <= 0 {
			grid[x][y]["fire"] = false
			grid[x][y]["intensity"] = 0
			msg["reply"].(chan Message) <- Message{"ok": true, "info": "already out", "intensity": 0}
			return
		}

		remove := 3
		if intensity < remove {
			remove = intensity
		}
		avail := manager["water"].(int)
		if avail < remove {
			remove = avail
		}
		if remove == 0 {
			msg["reply"].(chan Message) <- Message{"ok": false, "info": "no water"}
			return
		}

		manager["water"] = avail - remove
		intensity -= remove
		if intensity <= 0 {
			grid[x][y]["fire"] = false
			intensity = 0
		}

		grid[x][y]["intensity"] = intensity

		msg["reply"].(chan Message) <- Message{
			"ok":        true,
			"info":      "extinguished_step",
			"intensity": intensity,
		}
		return

	// TODO to complete Task 0: implementation of other messages for the following actions:
	// - truck requests position of the closes fire
	// - truck moves around the grid (n, s, e, w)
	// - truck requests water
	// - truck extinguishes fire
	default:
		msg["reply"].(chan Message) <- Message{"ok": false}
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
// TODO for Task 0: add/modify/expand as needed

func inBounds(size, x, y int) bool {
	return x >= 0 && x < size && y >= 0 && y < size
}

// Add new fire to the grid
func addFire(manager map[string]interface{}, x, y int) {
	grid := manager["grid"].([][]map[string]interface{})
	if !grid[x][y]["fire"].(bool) {
		grid[x][y]["fire"] = true
		grid[x][y]["intensity"] = rand.Intn(3) + 1
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
func createTruck(id string, x, y int, inbox chan Message) map[string]interface{} {
	return map[string]interface{}{
		"id":       id,
		"x":        x,
		"y":        y,
		"mgr":      inbox,
		"reply":    make(chan Message, 1),
		"water":    5,  // start with some water
		"maxWater": 10, // truck capacity (small for demo)
	}
}

// Register a firetruck to the grid via the manager
func registerTruck(truck map[string]interface{}) {
	msg := Message{
		"type":  "register",
		"from":  truck["id"],
		"x":     truck["x"],
		"y":     truck["y"],
		"reply": truck["reply"],
	}
	truck["mgr"].(chan Message) <- msg
	<-truck["reply"].(chan Message)
}

// Skeleton of firetruck actions
func truckLoop(truck map[string]interface{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for range ticker.C {

		// Ask manager for nearest fire from our current position

		req := Message{
			"type":  "nearest_fire",
			"from":  truck["id"],
			"x":     truck["x"],
			"y":     truck["y"],
			"reply": truck["reply"],
		}
		truck["mgr"].(chan Message) <- req

		resp := <-truck["reply"].(chan Message)

		if !resp["ok"].(bool) {
			fmt.Printf("[Truck %s] No fires found.\n", truck["id"])
			continue

			// (For later steps you’ll move toward (nx,ny), request water, etc.)
			// Keep it simple for now since we're only doing "find nearest".
		}

		targetX := resp["x"].(int)
		targetY := resp["y"].(int)

		curX := truck["x"].(int)
		curY := truck["y"].(int)
		if curX == targetX && curY == targetY {
			extReq := Message{
				"type":  "extinguish",
				"from":  truck["id"],
				"x":     curX,
				"y":     curY,
				"reply": truck["reply"],
			}
			truck["mgr"].(chan Message) <- extReq
			extResp := <-truck["reply"].(chan Message)

			if extResp["ok"].(bool) {
				fmt.Printf("[Truck %s] Extinguishing at (%d,%d). Intensity now: %v\n",
					truck["id"], curX, curY, extResp["intensity"])
			} else {
				fmt.Printf("[Truck %s] Extinguish failed: %v\n", truck["id"], extResp["info"])
			}
			continue
		}
		nx, ny := stepToward(curX, curY, targetX, targetY)

		moveReq := Message{
			"type":  "move",
			"from":  truck["id"],
			"x":     curX,
			"y":     curY,
			"nx":    nx,
			"ny":    ny,
			"reply": truck["reply"],
		}
		truck["mgr"].(chan Message) <- moveReq
		moveResp := <-truck["reply"].(chan Message)

		if moveResp["ok"].(bool) {
			truck["x"] = moveResp["x"].(int)
			truck["y"] = moveResp["y"].(int)
			fmt.Printf("[Truck %s] Moved to (%d,%d)\n", truck["id"], truck["x"], truck["y"])
		} else {
			fmt.Printf("[Truck %s] Move failed: %v\n", truck["id"], moveResp["info"])
		}
	}
}

// TODO for Task 0: add functionality:
// - decide movement direction
// - send move request
// - send water request
// - communicate
// - extinguish fire

// ----------------------- Main -----------------------
func main() {
	rand.Seed(time.Now().UnixNano())

	// Create the central manager (with arbitrary example values)
	manager := createManager(20, 500, 20)

	// Create the 'done' channel to signal when the simulation is over
	done := make(chan struct{})

	// Run the manager for 50 timesteps with 2 seconds per timestep
	go runManager(manager, 50, 2*time.Second, done)

	// Create two initial fires at random grid positions (example)
	addFire(manager, rand.Intn(20), rand.Intn(20))
	addFire(manager, rand.Intn(20), rand.Intn(20))

	// Create, register, and run 2 firetrucks at random grid positions (example)
	truck1 := createTruck("T1", rand.Intn(20), rand.Intn(20), manager["inbox"].(chan Message))
	truck2 := createTruck("T2", rand.Intn(20), rand.Intn(20), manager["inbox"].(chan Message))

	registerTruck(truck1)
	registerTruck(truck2)
	fmt.Println(manager)
	go truckLoop(truck1)
	go truckLoop(truck2)

	// Finish simulation once the manager signals completion
	<-done
}
