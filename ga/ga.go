package ga

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	netHttp "net/http"
	"sync"
	"time"
)

const baseURL = "https://www.google-analytics.com/collect"
const baseURLGA4 = "https://www.google-analytics.com/mp/collect"
const version = "1"
const hitType = "pageview"

var (
	mu sync.Mutex

	defaultTransport = &netHttp.Transport{
		Dial: (&net.Dialer{
			KeepAlive: 600 * time.Second}).Dial,
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
	}
	requestClient = &netHttp.Client{
		Transport: defaultTransport,
		Timeout:   2 * time.Second,
	}
)

// Analytics struct contains the information which can be sent to
type Analytics struct {
	ClientID        string `json:"client_id"`
	DataSource      string
	UserIP          string
	UserAgent       string
	DocumentReferer string
	CampaignName    string
	CampaignSource  string
	CampaignMedium  string
	CampaignKeyword string
	CampaignID      string
	DocumentHost    string
	DocumentPath    string
	DocumentTitle   string
}

type ga4EventParams struct {
	EngmtTime    int    `json:"engagement_time_msec"`
	PageTitle    string `json:"page_title,omitempty"`
	PageLocation string `json:"page_location,omitempty"`
}

type ga4Event struct {
	Name   string         `json:"name"`
	Params ga4EventParams `json:"params"`
}

type gA4Data struct {
	ClientID string     `json:"client_id"`
	UserID   string     `json:"user_id"`
	Events   []ga4Event `json:"events"`
}

type GA4Data struct {
	ClientID       string
	UserID         string
	OrgName        string
	TrackingDomain string
	Hostname       string
	Keyword        string
	Location       string
}

func (gd *GA4Data) createMPData() *gA4Data {
	var events []ga4Event
	events = append(events, ga4Event{
		Name: "page_view",
		Params: ga4EventParams{
			EngmtTime:    10,
			PageTitle:    gd.Hostname,
			PageLocation: gd.Location,
		},
	})
	if len(gd.TrackingDomain) > 0 {
		events = append(events, ga4Event{
			Name: "page_view",
			Params: ga4EventParams{
				EngmtTime:    10,
				PageTitle:    gd.TrackingDomain,
				PageLocation: gd.Location,
			},
		})
	}
	if len(gd.OrgName) > 0 {
		events = append(events, ga4Event{
			Name: gd.OrgName,
			Params: ga4EventParams{
				EngmtTime: 10,
			},
		})
	}
	if len(gd.Keyword) > 0 {
		events = append(events, ga4Event{
			Name: gd.Keyword,
			Params: ga4EventParams{
				EngmtTime: 10,
			},
		})
	}

	var d = &gA4Data{
		ClientID: gd.ClientID,
		UserID:   gd.UserID,
		Events:   events,
	}
	fmt.Printf("==ga4eventdata %+v\n", d)
	return d
}

// Queue struct will be used to send data to GA
type Queue struct {
	SendCount  int
	ResetCount int
	CC         int // current counter
	TrackingID string

	APISecret     string
	MeasurementID string
}

// Push method will send data to analytics by using basic sampling logic
func (queue *Queue) Push(data *Analytics) {
	mu.Lock()
	queue.CC++
	var currentGACount = queue.CC
	mu.Unlock()

	if currentGACount <= queue.SendCount {
		var req, err = netHttp.NewRequest("GET", baseURL, nil)
		if err != nil {
			return
		}
		q := req.URL.Query()
		q.Add("v", version)
		q.Add("tid", queue.TrackingID)
		q.Add("cid", data.ClientID)
		q.Add("uip", data.UserIP)
		q.Add("ua", data.UserAgent)
		q.Add("dr", data.DocumentReferer)
		q.Add("ds", data.DataSource)
		q.Add("ci", data.CampaignID)
		q.Add("cn", data.CampaignName)
		q.Add("cs", data.CampaignSource)
		q.Add("cm", data.CampaignMedium)
		q.Add("ck", data.CampaignKeyword)
		q.Add("t", hitType)
		q.Add("dh", data.DocumentHost)
		q.Add("dp", data.DocumentPath)
		q.Add("dt", data.DocumentTitle)
		req.URL.RawQuery = q.Encode()

		var resp, reqErr = requestClient.Do(req)
		if reqErr == nil {
			resp.Body.Close()
		}
	} else if currentGACount > queue.ResetCount {
		mu.Lock()
		queue.CC = 0
		mu.Unlock()
	}
}

func (queue *Queue) PushGA4(data *GA4Data) {
	mu.Lock()
	queue.CC++
	var currentGACount = queue.CC
	mu.Unlock()

	if currentGACount <= queue.SendCount {
		d := data.createMPData()
		jsonStr, jsonErr := json.Marshal(d)
		if jsonErr != nil {
			fmt.Printf("==jsonErr %+v\n", jsonErr)
			return
		}
		var req, err = netHttp.NewRequest("POST", baseURLGA4, bytes.NewBuffer(jsonStr))
		if err != nil {
			return
		}
		q := req.URL.Query()
		q.Add("api_secret", queue.APISecret)
		q.Add("measurement_id", queue.MeasurementID)
		req.URL.RawQuery = q.Encode()

		var resp, reqErr = requestClient.Do(req)
		if reqErr == nil {
			resp.Body.Close()
		}
	} else if currentGACount > queue.ResetCount {
		mu.Lock()
		queue.CC = 0
		mu.Unlock()
	}
}
