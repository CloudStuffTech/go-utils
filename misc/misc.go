package misc

func JoinKeyParts(parts []string) string {
	return StringJoin(parts, "|")
}

func StringJoin(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	out := parts[0]
	for i := 1; i < len(parts); i++ {
		out += sep + parts[i]
	}
	return out
}
