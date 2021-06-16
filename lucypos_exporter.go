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
    "path/filepath"

    "strings"
    "crypto/aes"
    "crypto/cipher"
    "crypto/rand"
	"encoding/hex"
	
    "errors"
    "io"
    "github.com/subosito/gotenv"

    "database/sql"

    "github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
    "github.com/prometheus/common/promlog"

    _ "github.com/go-sql-driver/mysql"
)

const (
    namespace = "lucypos" // For Prometheus metrics.
	key = "the-key-has-to-be-32-bytes-long!"
)

var (
    listeningAddress = flag.String("telemetry.address", ":9118", "Address on which to expose metrics.")
    metricsEndpoint  = flag.String("telemetry.endpoint", "/metrics", "Path under which to expose metrics.")
    backupDirectoryPath       = flag.String("backup_dir", "/home/jerome/projects/LucyPOS/lucypos/docker/monitoring/host", "Location of the backup file")
//    mysqlConnection       = flag.String("db_connection", "exporter:i97EXq0H@tcp(172.25.0.101:3306)/demo_tpv", "Connection to database")
//    mysqlSymdbConnection       = flag.String("symdb_connection", "exporter:i97EXq0H@tcp(172.25.0.101:3306)/demo_sym", "Connection to symdb database")
    mode = flag.String("mode", "server", "execution mode, can be server or encrypt.")
    plaintext = flag.String("plaintext", "", "plaintext text to be encrypted")
)

type Exporter struct {
    BackupDirectoryPath    string
    MysqlConnection    string
    MysqlSymdbConnection    string
    mutex  sync.Mutex

    backupSize *prometheus.Desc
    backupDate *prometheus.Desc
    lastAudit *prometheus.Desc
    symError *prometheus.Desc

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
        ch <- e.backupDate
        ch <- e.lastAudit
        ch <- e.symError
}

func NewExporter(backupDirectoryPath string, mysqlConnection string, mysqlSymdbConnection string) *Exporter {

    //Declare metrics that will be exposed to prometheus
    return &Exporter{
        BackupDirectoryPath: backupDirectoryPath,
        MysqlConnection: mysqlConnection,
        MysqlSymdbConnection: mysqlSymdbConnection,
        backupSize:  prometheus.NewDesc(
            prometheus.BuildFQName(namespace, "", "backup_file_size"),
            "Size of backup file",
            []string{"name"},
            nil),
        backupDate:  prometheus.NewDesc(
            prometheus.BuildFQName(namespace, "", "backup_file_date_mod"),
            "Date of backup file",
            []string{"name"},
            nil),
        lastAudit:  prometheus.NewDesc(
            prometheus.BuildFQName(namespace, "", "last_audit_timestamp"),
            "Rows sync in mysql",
            []string{"name"},
            nil),
        symError:  prometheus.NewDesc(
            prometheus.BuildFQName(namespace, "", "sym_error"),
            "Sync error in symdb",
            []string{"sqlError"},
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

	//find the index of the first .tar in that list
	i := 0
    for _, file := range files {
      if file.Mode().IsRegular() {
          if filepath.Ext(file.Name()) == ".tar" {
            fmt.Println(file.Name())
            break
          }
		  promlog.Infof("-regular file ")
      }
      i = i+1
    }
    // debug exception if no backup file at all
    promlog.Infof("i is %s ", i)
      
    size := files[i].Size()
    label := files[i].Name()

    val := float64(size)

    promlog.Infof("Last backup is %s with size ", label, val)

    ch <- prometheus.MustNewConstMetric(e.backupSize, prometheus.CounterValue, val, label)

    return nil;
}

func (e *Exporter) collectBackupDate(ch chan<- prometheus.Metric) error {

    files, err := ioutil.ReadDir(e.BackupDirectoryPath)
    if err != nil {
        ch <- prometheus.MustNewConstMetric(e.backupSize, prometheus.CounterValue, 0, "n/a")
        return fmt.Errorf("Error opening backup file: %v", err)
    }

    //get the last modified file in the directory
    sort.Sort(byModTime(files))

	//find the index of the first .tar in that list
	i := 0
    for _, file := range files {
      if file.Mode().IsRegular() {
          if filepath.Ext(file.Name()) == ".tar" {
            fmt.Println(file.Name())
            break
          }
      }
      i = i+1
    }
      
    date := files[i].ModTime()
    label := files[i].Name()

	//use the timestamp for timeline series
    val := float64(date.Unix())
    
// switched off this comment as could be guilty of halting the container with unkown char error
// error from daemon in stream: Error grabbing logs: invalid character '\x00' looking for beginning of value
//    promlog.Infof("Last backup is %s with date ", label, date)

    ch <- prometheus.MustNewConstMetric(e.backupDate, prometheus.CounterValue, val, label)

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

    promlog.Infof("Collected last timestamp metric %s", lastAudit)

    ch <- prometheus.MustNewConstMetric(e.lastAudit, prometheus.CounterValue, lastAudit, "audit")

    return nil

}


func (e *Exporter) collectSymError(ch chan<- prometheus.Metric) error {

    db, err := sql.Open("mysql", e.MysqlSymdbConnection)
    if err != nil {
        ch <- prometheus.MustNewConstMetric(e.symError, prometheus.CounterValue, 0, "symdb_out")
        return fmt.Errorf("Error connecting to mysql: %v", err)
    }
    defer db.Close()

    stmtOut, err := db.Prepare("select batch_id, sql_message from sym_outgoing_batch where error_flag=1")
    if err != nil {
        ch <- prometheus.MustNewConstMetric(e.symError, prometheus.CounterValue, 0, "symdb_out")
        return fmt.Errorf("Error preparing query to mysql: %v", err)
    }
    defer stmtOut.Close()

    var symSQLError string
    var batchId string
    var symError float64
  
    err = stmtOut.QueryRow().Scan(&batchId, &symSQLError)
    if err != nil {
        ch <- prometheus.MustNewConstMetric(e.symError, prometheus.CounterValue, 0, "symdb_out")
        promlog.Infof("Error querying mysql for outgoing batch errors: %v", err)
        //return fmt.Errorf("Error querying mysql for outgoing batch errors: %v", err)
    }else{
        promlog.Infof("look like there is an error in outgoing batch: %v", batchId)
    }

    //build a list of string labels (maybe another version of prom?)
	//labels := [...]string{batchId, symSQLError}
	
	labels := symSQLError
	
	symError = 0
	
	
	if batchId != "" {
		symError = 1
	}
	
	// v2.6 check incoming batches for errors
	if symError == 0 {
	    stmtIn, err := db.Prepare("select batch_id, sql_message from sym_incoming_batch where error_flag=1")
	    if err != nil {
	        ch <- prometheus.MustNewConstMetric(e.symError, prometheus.CounterValue, 0, "symdb_in")
	        return fmt.Errorf("Error preparing query to mysql: %v", err)
	    }
	    defer stmtIn.Close()

	    err = stmtIn.QueryRow().Scan(&batchId, &symSQLError)
	    if err != nil {
	        ch <- prometheus.MustNewConstMetric(e.symError, prometheus.CounterValue, 0, "symdb_in")
	        promlog.Infof("Error querying mysql for incoming batch errors: %v", err)
	        return fmt.Errorf("Error querying mysql for incoming batch errors: %v", err)
	    }else{
	        promlog.Infof("look like there is an error in incoming batch: %v", batchId)
	    }

		// labels := symSQLError

		if batchId != "" {
			labels = symSQLError
			symError = 1
		}
	}

    promlog.Infof("Collected symError Metric %s", symError)

    ch <- prometheus.MustNewConstMetric(e.symError, prometheus.CounterValue, symError, labels)

    return nil

}

func (e *Exporter) collect(ch chan<- prometheus.Metric) error {

    promlog.Infof("Starting collecting metrics")

    if err := e.collectBackup(ch); err != nil {
        return err
    }

    if err := e.collectBackupDate(ch); err != nil {
        return err
    }

    if err := e.collectDbAudit(ch); err != nil {
        return err
    }

    if err := e.collectSymError(ch); err != nil {
        return err
    }

    return nil;

}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {

    promlog.Infof("Locking mutex")

    e.mutex.Lock() // To protect metrics from concurrent collects.
    defer e.mutex.Unlock()
    if err := e.collect(ch); err != nil {
    //switched off as might be guilty of halting the container
//        promlog.Errorf("Error checking lucypos: %s", err)
    }
    return
}
// see https://astaxie.gitbooks.io/build-web-application-with-golang/en/09.6.html
func encrypt(plaintext []byte, key []byte) ([]byte, error) {
    c, err := aes.NewCipher(key)
    if err != nil {
        return nil, err
    }

    gcm, err := cipher.NewGCM(c)
    if err != nil {
        return nil, err
    }

    nonce := make([]byte, gcm.NonceSize())
    if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
        return nil, err
    }

    return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

func decrypt(ciphertext []byte, key []byte) ([]byte, error) {
    c, err := aes.NewCipher(key)
    if err != nil {
        return nil, err
    }

    gcm, err := cipher.NewGCM(c)
    if err != nil {
        return nil, err
    }

    nonceSize := gcm.NonceSize()
    if len(ciphertext) < nonceSize {
        return nil, errors.New("ciphertext too short")
    }

    nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
    return gcm.Open(nil, nonce, ciphertext, nil)
}
// check if password start with crypt://, if yes decrypt and return plain text
func decodePassword(password []byte) []byte{
	var retstring []byte

	if strings.HasPrefix(string(password[:]), "crypt://") {
		dbname := os.Getenv("TPVDB_NAME")
		// the key has to be 32 bytes long
		// fun with go, build a key that is specific to the client and 32 bit longs
		bkey := []byte(key)
		shorterkey := key[:len(bkey)-len(dbname)]
		finalkey := append([]byte(dbname), shorterkey...)
		
		// extract the encrypted part
		cipher := password[len([]byte("crypt://")):len(password)]
	    
		hexcipher, err := hex.DecodeString(string(cipher[:]))
	    if err != nil {
	        // TODO: Properly handle error
	        promlog.Fatal(err)
	    }
		
		decoded, err := decrypt(hexcipher, finalkey)
	    if err != nil {
	        // TODO: Properly handle error
	        promlog.Fatal(err)
	    }
	    retstring = decoded
	}else{
		retstring = password
	}
	
	return retstring
}
func init() {
	gotenv.Load()
}
func main() {
    flag.Parse()
    
	dbname := os.Getenv("TPVDB_NAME")
    // if flag ... then output encrypted pass and exit
    // else run in server mode
    if *mode == "server" {
		pass := os.Getenv("DB_PASS")
		
		decodedpass := decodePassword([]byte(pass))
	
		//build tpv connection string
		var myConnectionString strings.Builder
		myConnectionString.WriteString(os.Getenv("DB_USER"))	
		myConnectionString.WriteString(":")	
		myConnectionString.WriteString(string(decodedpass[:]))	
		myConnectionString.WriteString("@(")	
		myConnectionString.WriteString(os.Getenv("DB_HOST"))	
		myConnectionString.WriteString(":")	
		myConnectionString.WriteString(os.Getenv("DB_PORT"))	
		myConnectionString.WriteString(")/")	
		myConnectionString.WriteString(os.Getenv("TPVDB_NAME"))	
		
//		promlog.Info("db connection ", myConnectionString.String())

		//build symdb connection string
		var sb strings.Builder
		sb.WriteString(os.Getenv("DB_USER"))	
		sb.WriteString(":")	
		sb.WriteString(string(decodedpass[:]))	
		sb.WriteString("@(")	
		sb.WriteString(os.Getenv("DB_HOST"))	
		sb.WriteString(":")	
		sb.WriteString(os.Getenv("DB_PORT"))	
		sb.WriteString(")/")	
		sb.WriteString(os.Getenv("SYMDB_NAME"))	

	
	    exporter := NewExporter(*backupDirectoryPath, myConnectionString.String(), sb.String())
	    prometheus.MustRegister(exporter)
	
	    promlog.Infof("Starting Server: ", *listeningAddress)
	    http.Handle(*metricsEndpoint, promhttp.Handler())
	    promlog.Fatal(http.ListenAndServe(*listeningAddress, nil))
	}else{
		if *mode == "encrypt"{
			// fun with go, build a key that is specific to the client and 32 bit longs
			bkey := []byte(key)
			shorterkey := bkey[:len(bkey)-len(dbname)]
			finalkey := append([]byte(dbname), shorterkey...)
		  	
		  	// encrypt text passed via -plaintext flag
			ciphertext, err := encrypt([]byte(*plaintext), finalkey)
		    if err != nil {
		        // TODO: Properly handle error
		        promlog.Fatal(err)
		    }
			fmt.Printf("%s%x", "crypt://", ciphertext)
		
		}		
	}
}
