package main

import (
    "flag"
    "fmt"
    "sort"
    "os"
    "io/ioutil"
    "net/http"
    "sync"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/common/log"
)

const (
    namespace = "backup" // For Prometheus metrics.
)

var (
    listeningAddress = flag.String("telemetry.address", ":9118", "Address on which to expose metrics.")
    metricsEndpoint  = flag.String("telemetry.endpoint", "/metrics", "Path under which to expose metrics.")
    backupDirectoryPath       = flag.String("backup_dir", "/home/jerome/projects/LucyPOS/lucypos/docker/monitoring/host", "Location of the backup file")
)

type Exporter struct {
    BackupDirectoryPath    string
    mutex  sync.Mutex

    backupSize *prometheus.Desc
}

type byModTime []os.FileInfo

func (slice byModTime) Len() int {
    return len(slice)
}

func (slice byModTime) Less(i, j int) bool {
    return slice[i].ModTime().Unix() > slice[j].ModTime().Unix() 
}

func (slice byModTime) Swap(i, j int) {
    slice[i], slice[j] = slice[j], slice[i]
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
        ch <- e.backupSize
}

func NewExporter(backupDirectoryPath string) *Exporter {
    return &Exporter{
        BackupDirectoryPath: backupDirectoryPath,
        backupSize:  prometheus.NewDesc(
            prometheus.BuildFQName(namespace, "", "backup_file_size"),
            "Size of backup file",
            []string{"name"},
            nil),
    }
}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {

    files, err := ioutil.ReadDir(e.BackupDirectoryPath)
    if err != nil {
        ch <- prometheus.MustNewConstMetric(e.backupSize, prometheus.CounterValue, 0)
        return fmt.Errorf("Error opening backup file: %v", err)
    }

    sort.Sort(byModTime(files))

    size := files[0].Size()
    label := files[0].Name()

    val := float64(size)

    ch <- prometheus.MustNewConstMetric(e.backupSize, prometheus.CounterValue, val, label)

    return nil
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
    e.mutex.Lock() // To protect metrics from concurrent collects.
    defer e.mutex.Unlock()
    if err := e.collect(ch); err != nil {
        log.Errorf("Error checking backup file: %s", err)
    }
    return
}

func main() {
    flag.Parse()

    exporter := NewExporter(*backupDirectoryPath)
    prometheus.MustRegister(exporter)

    log.Infof("Starting Server: %s", *listeningAddress)
    http.Handle(*metricsEndpoint, prometheus.Handler())
    log.Fatal(http.ListenAndServe(*listeningAddress, nil))
}