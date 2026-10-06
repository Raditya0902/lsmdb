package raftnode

// QueuedEvents reports how many events wait in the runtime's queue. Tests use
// it to know that proposals sit behind a held persist before releasing it.
func QueuedEvents(r *Runtime) int { return len(r.events) }
