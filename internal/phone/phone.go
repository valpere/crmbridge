// Package phone normalizes Ukrainian phone numbers to the 380XXXXXXXXX form.
package phone

import "strings"

// Normalize keeps digits only and maps the common local spellings
// (+380…, 380…, 0XXXXXXXXX, 80XXXXXXXXX, XXXXXXXXX) to 380XXXXXXXXX.
// It returns "" when the result is not a plausible Ukrainian number.
func Normalize(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	d := b.String()
	switch {
	case len(d) == 12 && strings.HasPrefix(d, "380"):
	case len(d) == 11 && strings.HasPrefix(d, "80"):
		d = "3" + d
	case len(d) == 10 && strings.HasPrefix(d, "0"):
		d = "38" + d
	case len(d) == 9:
		d = "380" + d
	default:
		return ""
	}
	return d
}
