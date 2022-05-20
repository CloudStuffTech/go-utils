package cache

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/patrickmn/go-cache"
)

var mu sync.Mutex

type Client struct {
	prefix string
	client *cache.Cache
}

type Config struct {
	Prefix    string
	MCServer  string
	CacheTime int
	MaxConns  int
	Timeout   time.Duration
}

type MultiClient struct {
	prefix     string
	expiration int32
	client     *cache.Cache
	mc         *memcache.Client
}

func NewClient(prefix string, defCacheTime int) *Client {
	var cacheTime = time.Duration(defCacheTime) * time.Minute
	c := cache.New(cacheTime, 5*time.Minute)

	var cc = &Client{client: c, prefix: prefix}
	return cc
}

func (cc *Client) getKeyName(key string) string {
	return cc.prefix + "_" + key
}

func (cc *Client) Set(key string, val interface{}) {
	cc.client.SetDefault(cc.getKeyName(key), val)
}

func (cc *Client) SetWithExpire(key string, val interface{}, duration time.Duration) {
	cc.client.Set(cc.getKeyName(key), val, duration)
}

func (cc *Client) Get(key string) (interface{}, bool) {
	return cc.client.Get(cc.getKeyName(key))
}

func (cc *Client) Delete(key string) {
	cc.client.Delete(cc.getKeyName(key))
}

// NewMultiClient method will return a pointer to MultiClient object
func NewMultiClient(prefix, mcServer string, defCacheTime int) *MultiClient {
	var cacheTime = time.Duration(defCacheTime) * time.Minute
	c := cache.New(cacheTime, 5*time.Minute)
	mc := memcache.New(mcServer)
	mc.Timeout = 20 * time.Millisecond
	mc.MaxIdleConns = 1024

	var cc = &MultiClient{client: c, mc: mc, prefix: prefix, expiration: int32(defCacheTime * 36)}
	return cc
}

// NewMultiClientV2 method will return a pointer to MultiClient object
func NewMultiClientV2(opts *Config) *MultiClient {
	var cacheTime = time.Duration(opts.CacheTime) * time.Minute
	c := cache.New(cacheTime, 5*time.Minute)
	mc := memcache.New(opts.MCServer)
	mc.Timeout = 20 * time.Millisecond
	mc.MaxIdleConns = opts.MaxConns
	if opts.Timeout > 0 {
		mc.Timeout = opts.Timeout
	}

	var cc = &MultiClient{client: c, mc: mc, prefix: opts.Prefix, expiration: int32(opts.CacheTime * 36)}
	return cc
}

// GetInternalClient method will return the pointer to internal memory cache client
func (cc *MultiClient) GetInternalClient() *cache.Cache {
	return cc.client
}

func (cc *MultiClient) getKeyName(key string) string {
	return cc.prefix + "_" + key
}

// Set method will set the object in both memory cache and memcache
func (cc *MultiClient) Set(key string, val interface{}) {
	k := cc.getKeyName(key)
	cc.client.SetDefault(k, val)

	result, err := json.Marshal(val)
	if err == nil {
		cc.mc.Set(&memcache.Item{
			Key:        k,
			Value:      result,
			Expiration: cc.expiration,
		})
	}
}

// SetInMemory method will set the object in memory cache
func (cc *MultiClient) SetInMemory(key string, val interface{}) {
	k := cc.getKeyName(key)
	cc.client.Set(k, val, time.Duration(cc.expiration)*time.Second)
}

// DelFromMemory method will delete the object from memory
func (cc *MultiClient) DelFromMemory(key string) {
	k := cc.getKeyName(key)
	cc.client.Delete(k)
}

// SetWithExpire method will set the object in both memory cache and memcache
func (cc *MultiClient) SetWithExpire(key string, val interface{}, secs int) {
	k := cc.getKeyName(key)
	cc.client.Set(k, val, time.Duration(secs)*time.Second)

	result, err := json.Marshal(val)
	if err == nil {
		cc.mc.Set(&memcache.Item{
			Key:        k,
			Value:      result,
			Expiration: int32(secs),
		})
	}
}

// Get method tires to find the key from memory cache then check memcache
func (cc *MultiClient) Get(key string) (interface{}, bool) {
	k := cc.getKeyName(key)
	result, found := cc.client.Get(k)
	if found {
		return result, found
	}
	item, err := cc.mc.Get(k)
	if err == nil {
		var cacheObj interface{}
		err = json.Unmarshal(item.Value, cacheObj)
		if err == nil {
			return cacheObj, true
		}
	}

	return nil, false
}

// GetWithSet method tries to get the key from program memory cache and if
// it fails then tries memcache and if the item is found in memcache then it
// is set in program memory for faster lookup
func (cc *MultiClient) GetWithSet(key string, resultObj interface{}) (interface{}, bool) {
	k := cc.getKeyName(key)
	result, found := cc.client.Get(k)
	if found {
		return result, found
	}
	item, err := cc.mc.Get(k)
	if err == nil {
		mu.Lock()
		err = json.Unmarshal(item.Value, resultObj)
		mu.Unlock()
		if err == nil {
			cc.client.Set(k, resultObj, time.Duration(cc.expiration)*time.Second)
			return resultObj, true
		}
	}

	return nil, false
}

// GetSliceOrBytes method tries to get the key from program memory cache and if
// it fails then tries memcache and if the item is found in memcache then it
// is set in program memory for faster lookup
func (cc *MultiClient) GetSliceOrBytes(key string) (interface{}, bool) {
	k := cc.getKeyName(key)
	result, found := cc.client.Get(k)
	if found {
		return result, found
	}
	item, err := cc.mc.Get(k)
	if err == nil {
		return item.Value, true
	}

	return nil, false
}

// GetIntWithSet method tries to get the key from program memory cache and if
// it fails then tries memcache and if the item is found in memcache then it
// is set in program memory for faster lookup
func (cc *MultiClient) GetIntWithSet(key string, resultObj int64) (int64, bool) {
	k := cc.getKeyName(key)
	result, found := cc.client.Get(k)
	if found {
		r := result.(int64)
		return r, found
	}
	item, err := cc.mc.Get(k)
	if err == nil {
		mu.Lock()
		err = json.Unmarshal(item.Value, &resultObj)
		mu.Unlock()
		if err == nil {
			cc.client.Set(k, resultObj, 5*time.Minute)
			return resultObj, true
		}
	}

	return 0, false
}

// Delete method will remove the key from both memory cache and memcache
func (cc *MultiClient) Delete(key string) {
	k := cc.getKeyName(key)
	cc.client.Delete(k)
	cc.mc.Delete(k)
}
