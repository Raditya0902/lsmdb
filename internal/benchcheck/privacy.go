package benchcheck

import (
	"net"
	"regexp"
	"strings"
)

var (
	ipv4Pattern = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}\b`)
	// IPv6 candidates are runs of hex digits and colons; only those that
	// net.ParseIP accepts and that contain "::" or seven colons count, so
	// times such as 03:23:45 are not mistaken for addresses.
	ipv6Candidate = regexp.MustCompile(`[0-9A-Fa-f:]*:[0-9A-Fa-f:]*`)
	homePattern   = regexp.MustCompile(`(?:/home/|/Users/)[^/"\s\\]+|/root(?:/|"|$)`)
)

// scanPrivacy searches the whole file text, not just the environment block,
// for the checking machine's identity, any home directory, and IP addresses.
// Loopback addresses identify nothing, so on their own they only warn.
func scanPrivacy(text string, id Identity) Result {
	found, loopback := map[string]bool{}, map[string]bool{}
	classify := func(candidate string) {
		ip := net.ParseIP(candidate)
		switch {
		case ip == nil:
		case ip.IsLoopback() || ip.IsUnspecified():
			loopback[candidate] = true
		default:
			found["IP address "+candidate] = true
		}
	}
	for _, match := range ipv4Pattern.FindAllString(text, -1) {
		classify(match)
	}
	for _, match := range ipv6Candidate.FindAllString(text, -1) {
		if strings.Contains(match, "::") || strings.Count(match, ":") == 7 {
			classify(match)
		}
	}
	for _, match := range homePattern.FindAllString(text, -1) {
		found["home path "+strings.TrimSuffix(match, `"`)] = true
	}
	if home := strings.TrimSpace(id.Home); home != "" && home != "/" && strings.Contains(text, home) {
		found["home directory "+home] = true
	}
	for _, name := range identityWords(id) {
		if containsWord(text, name) {
			found["identity "+name] = true
		}
	}
	switch {
	case len(found) > 0:
		return Result{"privacy", Fail, "found " + strings.Join(sortedKeys(found), ", ")}
	case len(loopback) > 0:
		return Result{"privacy", Warn, "loopback addresses only: " + strings.Join(sortedKeys(loopback), ", ")}
	}
	return Result{"privacy", Pass, "no hostname, username, home path or IP address in the file"}
}

// identityWords returns the hostname, its first label, the username, and
// every forbidden word.
func identityWords(id Identity) []string {
	var words []string
	host := strings.TrimSpace(id.Hostname)
	if host != "" && host != "localhost" {
		words = append(words, host)
		if first, _, ok := strings.Cut(host, "."); ok && first != "" {
			words = append(words, first)
		}
	}
	for _, word := range append([]string{id.Username}, id.Forbidden...) {
		if word = strings.TrimSpace(word); word != "" {
			words = append(words, word)
		}
	}
	return words
}

// containsWord matches word case-insensitively where it is not part of a
// longer name made of letters, digits, '_' or '-'.
func containsWord(text, word string) bool {
	pattern := `(?i)(?:^|[^A-Za-z0-9_-])` + regexp.QuoteMeta(word) + `(?:$|[^A-Za-z0-9_-])`
	return regexp.MustCompile(pattern).MatchString(text)
}
