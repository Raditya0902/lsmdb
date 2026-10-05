//go:build !darwin

package benchenv

func darwinStatfs(string) (fsType, device string) { return "", "" }
