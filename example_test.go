package coordis

import (
	"fmt"
	"github.com/garyburd/redigo/redis" // Depends on Redigo
	"time"
)

// You have an automated kitchen with
// components connected to a Redis database.
// Before you can mix the ingredients,
// you need to boil the water and cut
// the vegetables. These tasks can be
// done in parallel, each by a different component.
// Imagine that each goroutine is running on a different machine.
// Please note that the underlying Redis library (Redigo)
// is not thread safe, so, in practice, a different Connection/Coordis
// pair would be needed for each thread.
func Example() {
	// First, connect to Redis
	c, _ := redis.Dial("tcp", ":6379")
	defer c.Close()

	// Create our Coordis
	coordis := NewCoordis(c, "kitchen")

	// Create our tasks
	cutCarrots := Task{"CutVegetable", "Carrot", nil}
	cutPeppers := Task{"CutVegetable", "Pepper", nil}
	boilWater := Task{"BoilWater", "Stove 3", nil}

	// We set the above as prerequisites to this task
	// by passing as the third argument
	mixIngredients := Task{"MixIngredients", "(Arbitrary Data)", []*Task{&cutCarrots, &cutPeppers, &boilWater}}

	coordis.Schedule(&mixIngredients)

	// Mixing goroutine
	go func() {
		// This will block until the vegetables are cut and the water has boiled
		taskId, _ := coordis.WaitForNext("MixIngredients", "IngredientMixer")
		fmt.Println("Mixing the ingredients")
		coordis.SetCompleted(taskId, "IngredientMixer")
		fmt.Println("Dinner is ready")
		coordis.Shutdown()
	}()

	// Water boiling goroutine
	go func() {
		boilWaterTaskId, _ := coordis.WaitForNext("BoilWater", "WaterBoiler")
		fmt.Println("Boiling the water")
		time.Sleep(time.Minute * 5) // Boil water for 5 minutes before signaling completion
		coordis.SetCompleted(boilWaterTaskId, "WaterBoiler")
	}()

	// Vegetable cutting goroutine
	go func() {
		for {
			cutVegetableTaskId, err := coordis.WaitForNext("CutVegetable", "VegetableCutter")
			if err == ErrShutdown {
				break
			}
			veggie, _ := coordis.GetData(cutVegetableTaskId)
			fmt.Printf("Cutting the %s\n", veggie)
		}
	}()
}
