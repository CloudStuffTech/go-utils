package time

import (
	"time"
	t "time"
)

// LoadTimeZones method will return a map containing all the timezones
func LoadTimeZones() map[string]*t.Location {
	zones := [...]string{"Pacific/Midway", "America/Adak", "Etc/GMT+10", "Pacific/Marquesas", "Pacific/Gambier", "America/Anchorage", "America/Ensenada", "Etc/GMT+8", "America/Los_Angeles", "America/Denver", "America/Chihuahua", "America/Dawson_Creek", "America/Belize", "America/Cancun", "Chile/EasterIsland", "America/Chicago", "America/New_York", "America/Havana", "America/Bogota", "America/Caracas", "America/Santiago", "America/La_Paz", "Atlantic/Stanley", "America/Campo_Grande", "America/Goose_Bay", "America/Glace_Bay", "America/St_Johns", "America/Araguaina", "America/Montevideo", "America/Miquelon", "America/Godthab", "America/Argentina/Buenos_Aires", "America/Sao_Paulo", "America/Noronha", "Atlantic/Cape_Verde", "Atlantic/Azores", "Europe/Belfast", "Europe/Dublin", "Europe/Lisbon", "UTC", "Europe/London", "Africa/Abidjan", "Europe/Amsterdam", "Europe/Belgrade", "Europe/Brussels", "Africa/Algiers", "Africa/Windhoek", "Asia/Beirut", "Africa/Cairo", "Asia/Gaza", "Africa/Blantyre", "Asia/Jerusalem", "Europe/Minsk", "Asia/Damascus", "Europe/Moscow", "Europe/Istanbul", "Africa/Addis_Ababa", "Asia/Tehran", "Asia/Dubai", "Asia/Yerevan", "Asia/Kabul", "Asia/Yekaterinburg", "Asia/Tashkent", "Asia/Kolkata", "Asia/Katmandu", "Asia/Dhaka", "Asia/Novosibirsk", "Asia/Rangoon", "Asia/Bangkok", "Asia/Krasnoyarsk", "Asia/Hong_Kong", "Asia/Irkutsk", "Australia/Perth", "Australia/Eucla", "Asia/Tokyo", "Asia/Seoul", "Asia/Yakutsk", "Australia/Adelaide", "Australia/Darwin", "Australia/Brisbane", "Australia/Hobart", "Asia/Vladivostok", "Australia/Lord_Howe", "Etc/GMT-11", "Asia/Magadan", "Pacific/Norfolk", "Asia/Anadyr", "Pacific/Auckland", "Etc/GMT-12", "Pacific/Chatham", "Pacific/Tongatapu", "Pacific/Kiritimati"}
	var timezoneLocs = make(map[string]*t.Location)
	for _, z := range zones {
		loc, err := t.LoadLocation(z)
		if err == nil {
			timezoneLocs[z] = loc
		}
	}
	return timezoneLocs
}

// GetInZone will return the time in the provided zone
func GetInZone(locs map[string]*t.Location, zone string) t.Time {
	loc, ok := locs[zone]
	if !ok {
		loc = time.UTC
	}
	nowTime := t.Now().In(loc)
	return nowTime
}

// GetTimeZone will return the zone in the provided zone
func GetTimeZone(locs map[string]*t.Location, zone string) *t.Location {
	loc, ok := locs[zone]
	if !ok {
		loc = time.UTC
	}
	return loc
}