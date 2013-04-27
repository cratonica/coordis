package coordis

// This defines a task to be scheduled
type Task struct {
	// The type of this task. Task queues are
	// referenced by task type. Can by any string,
	// or perhaps the ordinal of an enumerated type.
	Type string

	// Arbitrary data associated with the task. Place your
	// parameters or serialized JSON here.
	Data string

	// A list of tasks that must be completed before 
	// this task shows up in its type's ready queue. These can,
	// in-turn, have their own prerequisites, ad infinitum.
	// Two tasks cannot share the same prereq.
	Prereqs []*Task
}
