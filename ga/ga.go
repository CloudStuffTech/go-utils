package ga

import (
	netHttp "net/http"
	"sync"
)

const baseURL = "https://www.google-analytics.com/collect"
const version = "1"
const hitType = "pageview"

var (
	mu sync.Mutex
)

// Analytics struct contains the information which can be sent to
type Analytics struct {
	ClientID        string
	DataSource      string
	UserIP          string
	UserAgent       string
	DocumentReferer string
	CampaignName    string
	CampaignSource  string
	CampaignMedium  string
	CampaignID      string
	DocumentHost    string
	DocumentPath    string
	DocumentTitle   string
}

// Queue struct will be used to send data to GA
type Queue struct {
	SendCount  int
	ResetCount int
	CC         int // current counter
	TrackingID string
}

// Push method will send data to analytics by using basic sampling logic
func (queue *Queue) Push(data *Analytics) {
	mu.Lock()
	queue.CC++
	var currentGACount = queue.CC
	mu.Unlock()

	if currentGACount <= queue.SendCount {
		var req, err = netHttp.NewRequest("GET", baseURL, nil)
		if err == nil {
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
			q.Add("t", hitType)
			q.Add("dh", data.DocumentHost)
			q.Add("dp", data.DocumentPath)
			q.Add("dt", data.DocumentTitle)
			req.URL.RawQuery = q.Encode()

			var client = &netHttp.Client{}
			var resp, err = client.Do(req)
			if err == nil {
				resp.Body.Close()
			}
		}
	} else if currentGACount > queue.ResetCount {
		mu.Lock()
		queue.CC = 0
		mu.Unlock()
	}
}
