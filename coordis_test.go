package coordis

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"math/rand"
	"testing"
)

const prefix string = "_go_coordis_test"
const serverAddr string = ":6379"
const serverNet string = "tcp"
const totalTypes uint32 = 8
const totalIterations int = 5000

func TestCoordisGetAssigned(t *testing.T) {
	c, err := redis.Dial(serverNet, serverAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	err = FlushKeysByPrefix(c, prefix)
	if err != nil {
		t.Fatal(err)
	}
	tm := NewCoordis(c, prefix)
	assigned, _ := tm.GetAssignedTasks("testClient")
	if len(assigned) != 0 {
		t.Fail()
	}
	tm.Schedule(&Task{"A", "", nil})
	tm.Schedule(&Task{"A", "", nil})
	next0, _ := tm.WaitForNext("A", "testClient")
	assigned, _ = tm.GetAssignedTasks("testClient")
	if len(assigned) != 1 || assigned[0] != next0 {
		t.Fail()
	}
	next1, _ := tm.WaitForNext("A", "testClient")
	assigned, _ = tm.GetAssignedTasks("testClient")
	if len(assigned) != 2 || next0 != assigned[1] || next1 != assigned[0] {
		t.Fail()
	}
	// Make sure we get an error if it is the wrong client
	err = tm.SetCompleted(next0, "testClientInvalid")
	if err == nil {
		t.Fail()
	}
	if err = tm.SetCompleted(next0, "testClient"); err != nil {
		t.Fail()
	}
	assigned, _ = tm.GetAssignedTasks("testClient")
	if len(assigned) != 1 || next1 != assigned[0] {
		t.Fail()
	}
	tm.SetCompleted(next1, "testClient")
	assigned, _ = tm.GetAssignedTasks("testClient")
	if len(assigned) != 0 {
		t.Fail()
	}
}

func TestCoordisFuzz(t *testing.T) {
	mainC, err := redis.Dial(serverNet, serverAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer mainC.Close()
	err = FlushKeysByPrefix(mainC, prefix)
	if err != nil {
		t.Fatal(err)
	}
	mainTm := NewCoordis(mainC, prefix)
	var totalTasks int
	for i := 0; i < totalIterations; i++ {
		totalTasks++
		mainTask := Task{string(rand.Uint32() % totalTypes), fmt.Sprintf("Main-%d", i), nil}
		var subId int
		for rand.Uint32()%2 == 0 {
			totalTasks++
			subTask := Task{string(rand.Uint32() % totalTypes), fmt.Sprintf("Sub-%d-%d", i, subId), nil}
			subId++
			mainTask.Prereqs = append(mainTask.Prereqs, &subTask)
		}
		err := mainTm.Schedule(&mainTask)
		if err != nil {
			t.Fatal(err)
		}
	}
	successChannel := make(chan bool)
	doWork := func(i uint8) {
		name := fmt.Sprintf("Handler-%d", i)
		c, err := redis.Dial(serverNet, serverAddr)
		if err != nil {
			t.Log(err)
			successChannel <- false
			return
		}
		defer c.Close()
		tm := NewCoordis(c, prefix)
		for {
			success := func() bool {
				taskId, err := tm.WaitForNext(string(i), name)
				if err != nil {
					t.Log(err)
					return false
				}
				deets, err := tm.GetData(taskId)
				if err != nil {
					t.Log(err)
					return false
				}
				t.Logf("%s is processing %s (%s)\n", name, deets, taskId)
				err = tm.SetCompleted(taskId, name)
				if err != nil {
					t.Logf("ERR: %s [%v]\n", name, err)
					return false
				}
				return true
			}()
			successChannel <- success
		}
	}
	for i := uint8(0); i < uint8(totalTypes); i++ {
		go doWork(i)
	}
	for totalTasks > 0 {
		didSucceed := <-successChannel
		if !didSucceed {
			t.Fatal("Terminating due to failure from goroutine")
		}
		totalTasks -= 1
	}

	remaining, err := redis.Strings(mainC.Do("KEYS", prefix+":*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(remaining) != 0 {
		t.Fatal("Not all keys were processed. %d still remaining. Was another instance of this test running against the same database concurrently?", len(remaining))
	}
}
