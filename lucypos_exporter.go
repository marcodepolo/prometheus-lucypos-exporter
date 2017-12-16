package main

//
// Exposes monitoring metrics of LucyPOS to Prometheus
//

import (
    "flag"
    "fmt"
    "sort"
    "os"
    "io/ioutil"
    "net/http"
    "sync"

    "database/sql"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/common/log"

    _ "github.com/go-sql-driver/mysql"
)

const (
    namespace = "lucypos" // For Prometheus metrics.
)

var (
    listeningAddress = flag.String("telemetry.address", ":9118", "Address on which to expose metrics.")
    metricsEndpoint  = flag.String("telemetry.endpoint", "/metrics", "Path under which to expose metrics.")
    backupDirectoryPath       = flag.String("backup_dir", "/home/jerome/projects/LucyPOS/lucypos/docker/monitoring/host", "Location of the backup file")
    mysqlConnection       = flag.String("db_connection", "exporter:i97EXq0H@tcp(172.25.0.101:3306)/demo_tpv", "Connection to database")
)

type Exporter struct {
    BackupDirectoryPath    string
    MysqlConnection    string
    mutex  sync.Mutex

    backupSize *prometheus.Desc
    lastAudit *prometheus.Desc

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
        ch <- e.lastAudit
}

func NewExporter(backupDirectoryPath string, mysqlConnection string) *Exporter {

    //Declare metrics that will be exposed to prometheus
    return &Exporter{
        BackupDirectoryPath: backupDirectoryPath,
        MysqlConnection: mysqlConnection,
        backupSize:  prometheus.NewDesc(
            prometheus.BuildFQName(namespace, "", "backup_file_size"),
            "Size of backup file",
            []string{"name"},
            nil),
        lastAudit:  prometheus.NewDesc(
            prometheus.BuildFQName(namespace, "", "last_audit_timestamp"),
            "Rows sync in mysql",
            []string{"name"},
            nil),
    }
}

func (e *Exporter) collectBackup(ch chan<- prometheus.Metric) error {

    files, err := ioutil.ReadDir(e.BackupDirectoryPath)
    if err != nil {
        ch <- prometheus.MustNewConstMetric(e.backupSize, prometheus.CounterValue, 0, "n/a")
        return fmt.Errorf("Error opening backup file: %v", err)
    }

    //get the last modified file in the directory
    sort.Sort(byModTime(files))

    size := files[0].Size()
    label := files[0].Name()

    val := float64(size)

    log.Infof("Last backup is %s with size ", label, val)

    ch <- prometheus.MustNewConstMetric(e.backupSize, prometheus.CounterValue, val, label)

    return nil;
}

func (e *Exporter) collectDbAudit(ch chan<- prometheus.Metric) error {

    db, err := sql.Open("mysql", e.MysqlConnection)
    if err != nil {
        ch <- prometheus.MustNewConstMetric(e.lastAudit, prometheus.CounterValue, 0, "audit")
        return fmt.Errorf("Error connecting to mysql: %v", err)
    }
    defer db.Close()

    stmtOut, err := db.Prepare("SELECT UNIX_TIMESTAMP(max(time)) FROM audit")
    if err != nil {
        ch <- prometheus.MustNewConstMetric(e.lastAudit, prometheus.CounterValue, 0, "audit")
        return fmt.Errorf("Error preparing query to mysql: %v", err)
    }
    defer stmtOut.Close()

    var lastAudit float64
    err = stmtOut.QueryRow().Scan(&lastAudit)
    if err != nil {
        ch <- prometheus.MustNewConstMetric(e.lastAudit, prometheus.CounterValue, 0, "audit")
        return fmt.Errorf("Error querying mysql: %v", err)
    }

    log.Infof("Collected Metric %s", lastAudit)

    ch <- prometheus.MustNewConstMetric(e.lastAudit, prometheus.CounterValue, lastAudit, "audit")

    return nil

}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {

    log.Infof("Starting collecting metrics")

    if err := e.collectBackup(ch); err != nil {
        return err
    }

    if err := e.collectDbAudit(ch); err != nil {
        return err
    }

    return nil;

}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {

    log.Infof("Locking mutex")

    e.mutex.Lock() // To protect metrics from concurrent collects.
    defer e.mutex.Unlock()
    if err := e.collect(ch); err != nil {
        log.Errorf("Error checking backup file: %s", err)
    }
    return
}

func main() {
    flag.Parse()

    exporter := NewExporter(*backupDirectoryPath, *mysqlConnection)
    prometheus.MustRegister(exporter)

    log.Infof("Starting ********** Server: %s", *listeningAddress)
    http.Handle(*metricsEndpoint, prometheus.Handler())
    log.Fatal(http.ListenAndServe(*listeningAddress, nil))
}