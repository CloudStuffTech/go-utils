package proxydb

import (
	"errors"

	"github.com/etf1/ip2proxy"
)

type Client struct {
	dbpath string
	DB     *ip2proxy.DB
}

type Data struct {
	IP        string
	ProxyType string
	Country   string
}

func NewClient(dbpath string) *Client {
	var client = &Client{dbpath: dbpath}
	return client
}

func (c *Client) Open() error {
	var err error
	c.DB, err = ip2proxy.Open(c.dbpath)
	if err != nil {
		c.DB = nil
		return err
	}
	return nil
}

func (c *Client) proxyName(proxyType ip2proxy.ProxyType) string {
	if proxyType == ip2proxy.ProxyVPN {
		return "vpn"
	}
	if proxyType == ip2proxy.ProxyTOR {
		return "tor"
	}
	if proxyType == ip2proxy.ProxyDCH {
		return "dch"
	}
	if proxyType == ip2proxy.ProxyPUB {
		return "pub"
	}
	if proxyType == ip2proxy.ProxyWEB {
		return "web"
	}
	return ""
}

func (c *Client) GetData(ipaddr string) (*Data, error) {
	if c.DB == nil {
		return nil, errors.New("Database is not opened")
	}
	var d = &Data{IP: ipaddr}
	result, err := c.DB.LookupIPV4Dot(ipaddr)
	if err != nil {
		return d, err
	}
	if result == nil || result.CountryCode == nil {
		return nil, errors.New("Not a proxy IP")
	}
	countryCode := result.CountryCode
	d.Country = *countryCode
	var proxyType = c.proxyName(result.Proxy)
	if len(proxyType) == 0 {
		return nil, errors.New("Invalid proxy Type")
	}
	d.ProxyType = proxyType
	return d, nil
}
