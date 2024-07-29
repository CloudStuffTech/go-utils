package ip

import (
	"net"
	"strings"
)

// IsInternalIP method checks whether the IP supplied is internal
func IsInternalIP(ip string) bool {
	var ipaddr = net.ParseIP(ip)
	_, private24BitBlock, _ := net.ParseCIDR("10.0.0.0/8")
	_, private20BitBlock, _ := net.ParseCIDR("172.16.0.0/12")
	_, private16BitBlock, _ := net.ParseCIDR("192.168.0.0/16")
	return private24BitBlock.Contains(ipaddr) || private20BitBlock.Contains(ipaddr) || private16BitBlock.Contains(ipaddr) || ip == "127.0.0.1" || ip == "::1"
}

// MaskIP removes the last octet from the IP address if it is a valid IP address
func MaskIP(ip string) string {
	var ipaddr = net.ParseIP(ip)
	var ipv4 = ipaddr.To4()
	var ipv6 = ipaddr.To16()
	if ipv4 != nil {
		var parts = strings.Split(ip, ".")
		var length = len(parts)
		parts[length-1] = "0"

		ip = strings.Join(parts, ".")
	} else if ipv6 != nil {
		var parts = strings.Split(ip, ":")
		var length = len(parts)
		parts[length-1] = "0000"

		ip = strings.Join(parts, ":")
	}

	return ip
}
