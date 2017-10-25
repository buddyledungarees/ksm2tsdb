package main

import (
	"context"
	"github.com/bluebreezecf/opentsdb-goclient/client"
	_ "github.com/bluebreezecf/opentsdb-goclient/config"
	"github.com/prometheus/client_golang/api"
	ipc "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"log"
	"net/http"
	"os"
	"strings"
)

var transformChan chan []*client.DataPoint
var logger *log.Logger

func init() {
	logger = log.New(os.Stdout, "logger: ", log.LstdFlags|log.LUTC|log.Lshortfile)
}

/*func create_tsdb_client(conf config.OpenTSDBConfig) *client.Client {
	tsdb_host := "localhost:4242"

	if conf == nil {
		var tsdb_conf config.OpenTSDBConfig
		tsdb_conf.OpentsdbHost = tsdb_host
		tsdb_conf.MaxPutPointsNum = 10000
		tsdb_conf.DetectDeltaNum = 100
	}
	tsdb_client, err := client.NewClient(*conf)
	if err != nil {
		logger.Panicf("Error creating tsdb client: %v", err)
	}
	return *tsdb_client
}
*/

func getMetrics(hostname string, endpoint string) chan []*client.DataPoint {
	ksmconf := api.Config{}
	ksmconf.Address = hostname
	logger.Printf("Using kube state metrics host: %v", ksmconf.Address)
	ksmclient, err := api.NewClient(ksmconf)
	if err != nil {
		logger.Panicf("Error creating new client: %v", err)
	}

	r, err := http.NewRequest("GET", ksmclient.URL(endpoint, nil).String(), nil)
	if err != nil {
		logger.Panicf("Error creating http request: %v", err)
	}
	ctx := context.Background()
	resp, data, err := ksmclient.Do(ctx, r)
	if err != nil {
		logger.Panicf("Error making request: %v", err)
	}
	logger.Printf("%v", resp)

	var parse = new(expfmt.TextParser)
	text, err := parse.TextToMetricFamilies(strings.NewReader(string(data)))
	if err != nil {
		logger.Panicf("error converting to metric families: %v", err)
	}
	//mf_output <- text
	dp := make(chan []*client.DataPoint)
	go func() { transform2datapoint(text, dp) }()
	return dp
}

func parseMetricFamily(mf *ipc.MetricFamily) []*client.DataPoint {
	var dps []*client.DataPoint
	for _, m := range mf.Metric {
		var dp client.DataPoint
		dp.Metric = *mf.Name
		dp.Tags = make(map[string]string)
		dp.Timestamp = int64(m.GetTimestampMs() / 1000)
		switch *mf.Type {
		case ipc.MetricType_COUNTER:
			dp.Value = m.Counter.Value
		case ipc.MetricType_GAUGE:
			dp.Value = m.Gauge.Value
		/*case 2:
			logger.Printf("Found %s for metric %s\n", metric_type.String(), name)
			value := m.GetSummary().GetValue
		case 3:
			logger.Printf("Found %s for metric %s\n", metric_type.String(), name)
			value := m.GetUntyped().GetValue
		case 4:
			logger.Printf("Found %s for metric %s\n", metric_type.String(), name)
			value := m.GetHistogram().GetValue */
		default:
			logger.Printf("Unknown metric type: %v for metric %s, skipping\n", mf.Type, *mf.Name)
			continue
		}
		for _, k := range m.Label {

			dp.Tags[*k.Name] = *k.Value
		}
		dps = append(dps, &dp)
	}
	return dps
}

func transform2datapoint(mf map[string]*ipc.MetricFamily, output chan []*client.DataPoint) {
	for _, mf2 := range mf {
		//logger.Printf("%s\n", k)
		dp := parseMetricFamily(mf2)
		output <- dp
	}
	close(output)
	logger.Printf("Sent all datapoints")
}

func main() {

	var (
		hostname = "http://10.96.200.102:8080"
		endpoint = "/metrics"
		//tsdb_host   = "localhost:4242"
		//global_tags []string
	)

	transformChan := getMetrics(hostname, endpoint)
	for p := range transformChan {
		logger.Printf("\n%v\n", p)
	}

}
