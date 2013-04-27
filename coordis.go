/*
Package coordis exposes a simple API for coordinating tasks
in a distributed system using redis as a mediator.

Author: Clint Caywood

Origin: http://github.com/cratonica/coordis
*/
package coordis

import "errors"

var ErrShutdown = errors.New("Coordis: Shutdown() called")

// The main type for this library
type Coordis interface {

	// Schedules the given task and attached prerequisites
	Schedule(task *Task) error

	// Gets the data associated with a task
	GetData(taskId string) (string, error)

	// Removes the task and updates the status of any tasks that are now unblocked
	// if this task was the final prerequisite. Must pass the assignee for validation.
	SetCompleted(taskId, assignedTo string) error

	// Waits for the next available task of the given type,
	// blocks until one is available or Shutdown is called.
	// assignTo is the identifier for the consumer
	// that will be used to recall assigned tasks
	// using GetAssignedTasks, and to call SetCompleted.
	// Returns the task id of the newly assigned task.
	WaitForNext(theType string, assignTo string) (string, error)

	// Gets all task ids currently assigned to the given entity.
	GetAssignedTasks(who string) ([]string, error)

	// Shuts down everyone who is waiting for tasks to complete.
	// Blocks until all waiters have exited
	Shutdown()
}
