//go:build !race

package cluster

// raceEnabled reports whether the test binary was built with -race.
const raceEnabled = false
