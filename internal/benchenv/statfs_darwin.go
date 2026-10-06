//go:build darwin

package benchenv

import "syscall"

// darwinStatfs returns the filesystem type and source device holding dir.
func darwinStatfs(dir string) (fsType, device string) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return "", ""
	}
	return cString(stat.Fstypename[:]), cString(stat.Mntfromname[:])
}

func cString(raw []int8) string {
	out := make([]byte, 0, len(raw))
	for _, c := range raw {
		if c == 0 {
			break
		}
		out = append(out, byte(c))
	}
	return string(out)
}
