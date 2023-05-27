package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Datastore struct {
	data     map[string]*DataEntry
	mutex    sync.RWMutex
	commands chan *Command
}

type DataEntry struct {
	value     string
	expiresAt time.Time
	mutex     sync.Mutex
}

type Command struct {
	Action    string
	Key       string
	Value     string
	Expiry    time.Duration
	Condition string
}

func NewDatastore() *Datastore {
	ds := &Datastore{
		data:     make(map[string]*DataEntry),
		commands: make(chan *Command),
	}

	go ds.processCommands()

	return ds
}

func (ds *Datastore) processCommands() {
	for cmd := range ds.commands {
		switch strings.ToUpper(cmd.Action) {
		case "SET":
			ds.set(cmd)
		case "GET":
			ds.get(cmd)
		case "QPUSH":
			ds.qpush(cmd)
		case "QPOP":
			ds.qpop(cmd)
		case "BQPOP":
			ds.bqpop(cmd)
		default:
			log.Printf("Unknown command: %s\n", cmd.Action)
		}
	}
}

func (ds *Datastore) set(cmd *Command) {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	entry := &DataEntry{
		value: cmd.Value,
	}

	if cmd.Expiry > 0 {
		entry.expiresAt = time.Now().Add(cmd.Expiry)
	}

	_, exists := ds.data[cmd.Key]
	if exists && strings.ToUpper(cmd.Condition) == "NX" {
		return // Key already exists, and condition is NX, so skip setting the value
	}

	ds.data[cmd.Key] = entry
}

func (ds *Datastore) get(cmd *Command) {
	ds.mutex.RLock()
	defer ds.mutex.RUnlock()

	entry, exists := ds.data[cmd.Key]
	if !exists {
		log.Printf("Key not found: %s\n", cmd.Key)
		return
	}

	if entry.expiresAt.IsZero() || entry.expiresAt.After(time.Now()) {
		log.Printf("Value for key %s: %s\n", cmd.Key, entry.value)
	} else {
		log.Printf("Key has expired: %s\n", cmd.Key)
	}
}

func (ds *Datastore) qpush(cmd *Command) {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	entry, exists := ds.data[cmd.Key]
	if !exists {
		entry = &DataEntry{}
		ds.data[cmd.Key] = entry
	}

	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	entry.value += " " + cmd.Value
}

func (ds *Datastore) qpop(cmd *Command) {
	ds.mutex.Lock()
	defer ds.mutex.Unlock()

	entry, exists := ds.data[cmd.Key]
	if !exists || entry.value == "" {
		log.Printf("Queue is empty: %s\n", cmd.Key)
		return
	}

	entry.mutex.Lock()
	defer entry.mutex.Unlock()

	values := strings.Fields(entry.value)
	value := values[len(values)-1]
	values = values[:len(values)-1]
	entry.value = strings.Join(values, " ")

	log.Printf("Popped value from queue %s: %s\n", cmd.Key, value)
}

func (ds *Datastore) bqpop(cmd *Command) {
	entry, exists := ds.data[cmd.Key]
	if !exists {
		entry = &DataEntry{}
		ds.data[cmd.Key] = entry
	}

	entry.mutex.Lock()

	if entry.value != "" {
		values := strings.Fields(entry.value)
		if len(values) > 0 {
			poppedValue := values[len(values)-1]
			values = values[:len(values)-1]
			entry.value = strings.Join(values, " ")

			entry.mutex.Unlock()

			log.Printf("Popped value from blocking queue %s: %s\n", cmd.Key, poppedValue)
			return
		}
	}

	// Queue is empty, block and wait for value to be pushed
	notifyCh := make(chan bool)

	go func() {
		ds.mutex.Lock()
		defer ds.mutex.Unlock()

		entry.mutex.Lock()
		defer entry.mutex.Unlock()

		if entry.value == "" {
			timer := time.NewTimer(cmd.Expiry)
			select {
			case <-ds.commands:
			case <-timer.C:
			}
		}

		notifyCh <- true
	}()

	select {
	case <-notifyCh:
		entry.mutex.Unlock()
		log.Printf("Blocking queue read operation timed out: %s\n", cmd.Key)
	case <-time.After(cmd.Expiry):
		entry.mutex.Unlock()
		log.Printf("Blocking queue read operation timed out: %s\n", cmd.Key)
	}
}

func parseCommand(input string) (*Command, error) {
	parts := strings.Fields(input)
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid command")
	}

	cmd := &Command{
		Action: strings.ToUpper(parts[0]),
		Key:    parts[1],
	}

	if cmd.Action == "SET" {
		if len(parts) < 3 {
			return nil, fmt.Errorf("invalid command")
		}

		cmd.Value = parts[2]

		for i := 3; i < len(parts); i++ {
			switch strings.ToUpper(parts[i]) {
			case "EX":
				if len(parts) > i+1 && strings.HasPrefix(parts[i+1], "NX") {
					cmd.Condition = "NX"
					i++
				} else if len(parts) > i+1 && strings.HasPrefix(parts[i+1], "XX") {
					cmd.Condition = "XX"
					i++
				} else {
					duration, err := strconv.Atoi(parts[i+1])
					if err != nil {
						return nil, fmt.Errorf("invalid expiry time")
					}
					cmd.Expiry = time.Duration(duration) * time.Second
					i++
				}
			case "NX":
				cmd.Condition = "NX"
			case "XX":
				cmd.Condition = "XX"
			}
		}
	}

	return cmd, nil
}

func handleCommand(w http.ResponseWriter, r *http.Request, ds *Datastore) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var requestData struct {
		Command string `json:"command"`
	}

	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	cmd, err := parseCommand(requestData.Command)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ds.commands <- cmd

	w.WriteHeader(http.StatusOK)
}

func main() {
	port := ":8080"

	ds := NewDatastore()

	http.HandleFunc("/command", func(w http.ResponseWriter, r *http.Request) {
		handleCommand(w, r, ds)
	})

	log.Printf("Server is running at port %s\n", port)

	log.Fatal(http.ListenAndServe(port, nil))
}
