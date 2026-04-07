package proxydb

import (
	"errors"
	"os"
	"syscall"

	"github.com/etf1/ip2proxy"
)

type Client struct {
	dbpath   string
	DB       *ip2proxy.DB
	mmapData []byte
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

func openMmap(c *Client) error {
	file, err := os.Open(c.dbpath)
	if err != nil {
		return err
	}
	defer file.Close() // Safe to close the file descriptor after mapping

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	// Map the file directly into virtual memory (Bypassing Go's Heap)
	c.mmapData, err = syscall.Mmap(
		int(file.Fd()),
		0,
		int(stat.Size()),
		syscall.PROT_READ,  // Read-only memory
		syscall.MAP_SHARED, // Share with OS page cache
	)
	if err != nil {
		return err
	}

	c.DB, err = ip2proxy.FromBytes(c.mmapData)
	if err != nil {
		// Clean up the OS memory if parsing fails
		syscall.Munmap(c.mmapData)
		c.mmapData = nil
		return err
	}
	return nil
}

func (c *Client) Open() error {
	err := openMmap(c)
	return err
	// var err error
	// c.DB, err = ip2proxy.Open(c.dbpath)
	// if err != nil {
	// 	c.DB = nil
	// 	return err
	// }
	// return nil
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
