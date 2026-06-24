package maxmind

import (
	"fmt"
	"net"

	"github.com/oschwald/geoip2-golang"
)

var cityDb *geoip2.Reader
var ispDb *geoip2.Reader
var connDb *geoip2.Reader

type City struct {
	Name, State, Country string
	Latitude, Longitude  float64
}

func Connect(cityDbPath, ispDbPath, connDbPath string) {
	var err error
	cityDb, err = geoip2.Open(cityDbPath)
	if err != nil {
		panic(err)
	}

	ispDb, err = geoip2.Open(ispDbPath)
	if err != nil {
		panic(err)
	}

	connDb, err = geoip2.Open(connDbPath)
	if err != nil {
		panic(err)
	}
}

func CityData(ip string) City {
	netIP := net.ParseIP(ip)
	record, err := cityDb.City(netIP)
	if err != nil || record == nil {
		cityObj := City{"XX", "XX", "XX", 0, 0}
		return cityObj
	}
	cityObj := City{}

	cityObj.Name = record.City.Names["en"]
	cityObj.Country = record.Country.IsoCode
	if len(record.Subdivisions) > 0 {
		cityObj.State = record.Subdivisions[0].Names["en"]
	}
	cityObj.Latitude = record.Location.Latitude
	cityObj.Longitude = record.Location.Longitude
	return cityObj
}

func ConnType(ip string) string {
	netIP := net.ParseIP(ip)
	record, err := connDb.ConnectionType(netIP)
	if err != nil {
		return ""
	}
	return record.ConnectionType
}

func ISP(ip string) string {
	netIP := net.ParseIP(ip)
	record, err := ispDb.ISP(netIP)
	if err != nil {
		return "XX"
	}
	return record.ISP
}

func Postal(ip string) string {
	if cityDb == nil || len(ip) == 0 {
		return ""
	}
	netIP := net.ParseIP(ip)
	if netIP == nil {
		return ""
	}
	record, err := cityDb.City(netIP)
	if err != nil || record == nil {
		return ""
	}
	return record.Postal.Code
}

// Close method will close the maxmind db files
func Close() {
	fmt.Println("closing maxmind db files!!")
	if cityDb != nil {
		cityDb.Close()
	}
	if ispDb != nil {
		ispDb.Close()
	}
	if connDb != nil {
		connDb.Close()
	}
}
