package main

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/x509"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"flag"
	"github.com/DataDog/zstd"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

var intermediateLock sync.Mutex
var intermediates *x509.CertPool
var rootsLock sync.Mutex
var roots *x509.CertPool
var validateCerts bool
var useTrustStore bool

func main() {
	rootStore := flag.String("trust-store", "", "The Trust store to use (optional)")
	input := flag.String("input", "", "input csv [.zst] with certificates in cert column (or '-' for stdin)")
	output := flag.String("output", "", "Output [.zst] File (optional)")
	noHeader := flag.Bool("no-header", false, "Disable csv header (no header in stdin mode)")
	verificationTimeStr := flag.String("time", "", "Time to use for verification purpose")
	prettyLogs := flag.Bool("pretty-log", false, "Format the output log in a colorful console text")
	certColumn := flag.Int("cert-column", -1, "Provide column for the certificates. Necessary for stdin")
	flag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *prettyLogs {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	}

	if *input == "" {
		log.Fatal().Msg("Pleas specify input")
	}
	if *verificationTimeStr != "" {
		validateCerts = true
	}
	if *rootStore != "" {
		useTrustStore = true
		log.Info().Msg("Using root certs from csv")
	}
	if *input == "-" {
		if validateCerts {
			log.Fatal().Msg("Cannot validate certs in stdin mode")
		}
		if *certColumn < 0 {
			log.Fatal().Msg("Please provide --cert-column in stdin mode")
		}
	}

	intermediates = x509.NewCertPool()
	roots = x509.NewCertPool()

	var verificationTime time.Time

	if validateCerts {
		var err error
		verificationTime, err = time.Parse("2006-01-02 15:04", *verificationTimeStr)
		if err != nil {
			log.Fatal().Err(err).Msg("Could not parse verification time")
		}

		if useTrustStore {
			processRootStore(*rootStore)
		}
		parseIntermediateCerts(*input)
	}

	parseCerts(*input, *output, *certColumn, verificationTime, !*noHeader)
}

func parseIntermediateCerts(input string) {
	var inputFile io.ReadCloser
	inputFile, _ = os.Open(input)
	if strings.HasSuffix(input, ".zst") {
		inputFile = zstd.NewReader(inputFile)
	}
	defer inputFile.Close()

	csvReader := csv.NewReader(inputFile)
	header := readHeader(csvReader)

	rootStoreColumn := inferColumn(header, "system_cert_store")
	certColumn := inferColumn(header, "cert")

	processCerts(csvReader, certColumn, rootStoreColumn, runIntermediateCollector)
}

func processRootStore(rootStorePath string) {
	rootLock := sync.Mutex{}
	processFunc := func(certRow []string, certColumn int, _ int) {
		rootLock.Lock()
		cert := parseCert(strings.ReplaceAll(certRow[certColumn], "'", ""))
		if cert != nil {
			roots.AddCert(cert)
		}
		rootLock.Unlock()
	}

	rootStoreReader, _ := os.Open(rootStorePath)
	csvReader := csv.NewReader(rootStoreReader)
	header := readHeader(csvReader)
	certColumn := inferColumn(header, "PEM")

	processCerts(csvReader, certColumn, -1, processFunc)
}

func parseCerts(input string, output string, certColumn int, verificationTime time.Time, showHeader bool) {

	verifyOpts := x509.VerifyOptions{
		Intermediates: intermediates,
		Roots:         roots,
		CurrentTime:   verificationTime,
	}

	outputChan := make(chan []string, 1000)

	var inputFile io.ReadCloser
	if input == "-" {
		inputFile = os.Stdin
		showHeader = false
	} else {
		inputFile, _ = os.Open(input)
		if strings.HasSuffix(input, ".zst") {
			inputFile = zstd.NewReader(inputFile)
		}
		defer inputFile.Close()

	}
	csvReader := csv.NewReader(inputFile)
	var header []string
	if input != "-" {
		header = readHeader(csvReader)
	}
	if certColumn < 0 {
		certColumn = inferColumn(header, "cert")
	}

	var outputStream io.Writer
	if output == "-" || output == "" {
		outputStream = os.Stdout
	} else {
		outputFile, err := os.Create(output)
		defer outputFile.Close()
		outputStream = outputFile
		if err != nil {
			log.Fatal().Err(err).Msg("Could not create output file")
		}
	}

	csvWriter := csv.NewWriter(outputStream)

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		var outHeader []string
		for _, c := range header {
			if c != "cert" {
				outHeader = append(outHeader, c)
			}
		}
		outHeader = append(outHeader,
			"sha1",
			"sha256",
			"valid",
			"verifyError",
			"subject",
			"issuer",
			"notBefore",
			"notAfter",
			"basicConstraints",
			"isCa",
			"sha256PubKey",
			"numAltNames",
			"altNames",
		)
		if showHeader {
			err := csvWriter.Write(outHeader)
			if err != nil {
				log.Fatal().Err(err).Msg("Could not write to output")
			}
		}
		numCerts := 0
		for o := range outputChan {
			numCerts++
			err := csvWriter.Write(o)
			if err != nil {
				log.Fatal().Err(err).Msg("Could not write to output")
			}
		}
		csvWriter.Flush()
		log.Info().Int("NumCerts", numCerts).Msg("Written all Certificates")
		wg.Done()
	}()

	numCerts := 0

	parseFunc := func(certRow []string, certColumn int, rootStoreColumn int) {
		var out []string
		cert := parseCert(certRow[certColumn])
		if cert != nil {
			for i, c := range certRow {
				if i != certColumn {
					out = append(out, c)
				}
			}
			dnsDames := ""
			names, err := json.Marshal(cert.DNSNames)
			if err != nil {
				log.Error().Err(err).Msg("Error parsing dns names")
			} else {
				dnsDames = string(names)
			}
			verifyErrString := ""
			valid := ""
			if validateCerts {
				verifyErrString = verifyCert(cert, verifyOpts)
				valid = strconv.FormatBool(verifyErrString == "")
			}

			out = append(out,
				hex.EncodeToString(GetSHA1(cert.Raw)),
				hex.EncodeToString(GetSHA256(cert.Raw)),
				valid,
				verifyErrString,
				strings.ReplaceAll(cert.Subject.String(), "\r", "\\r"),
				strings.ReplaceAll(cert.Issuer.String(), "\r", "\\r"),
				cert.NotBefore.Format(time.RFC3339),
				cert.NotAfter.Format(time.RFC3339),
				strconv.FormatBool(cert.BasicConstraintsValid),
				strconv.FormatBool(cert.IsCA),
				hex.EncodeToString(GetSHA256(cert.RawSubjectPublicKeyInfo)),
				strconv.Itoa(len(cert.DNSNames)),
				dnsDames,
			)
			outputChan <- out
			numCerts++
		} else {
			log.Debug().Msg("Cert was null")
		}
	}

	processCerts(csvReader, certColumn, -1, parseFunc)
	log.Info().Int("Num Certs", numCerts).Msg("Processed Certs")
	close(outputChan)

	wg.Wait()
}

func readHeader(reader *csv.Reader) []string {
	header, err := reader.Read()
	if err != nil {
		log.Fatal().Err(err).Msg("Could not read from certs csv")
	}
	return header
}

func inferColumn(header []string, column string) int {
	for i, h := range header {
		if h == column {
			return i
		}
	}
	log.Fatal().Str("inferColumn", column).Strs("header", header).Msg("Could not infer header column")
	return -1
}

func verifyCert(cert *x509.Certificate, opts x509.VerifyOptions) (verifyErr string) {
	verifyErr = "golang panic"
	defer func() {
		if r := recover(); r != nil {
			log.Warn().Interface("r", r).Msg("Recovered in verifyCert")
		}
	}()
	_, err := cert.Verify(opts)
	if err != nil {
		verifyErr = err.Error()
	} else {
		verifyErr = ""
	}
	return
}

type certProcessor func([]string, int, int)

func processCerts(csvReader *csv.Reader, certColumn int, rootStoreColumn int, runOnCerts certProcessor) {

	certs := make(chan []string, 1000)

	wg := sync.WaitGroup{}

	for i := 0; i < runtime.NumCPU(); i++ {
		wg.Add(1)
		go func() {
			for certRow := range certs {
				runOnCerts(certRow, certColumn, rootStoreColumn)
			}
			wg.Done()
		}()
	}

	numCerts := 0

	for line, err := csvReader.Read(); err != io.EOF; line, err = csvReader.Read() {
		certs <- line
		numCerts++
	}
	log.Info().Int("NumCerts", numCerts).Msg("Read Certificates")
	close(certs)
	wg.Wait()
}

func runIntermediateCollector(cert_row []string, certColumn int, rootStoreColumn int) {
	cert := parseCert(cert_row[certColumn])
	if cert != nil && cert.IsCA {
		intermediateLock.Lock()
		intermediates.AddCert(cert)
		intermediateLock.Unlock()
	}
	if !useTrustStore && cert != nil && cert_row[rootStoreColumn] == "1" {
		rootsLock.Lock()
		roots.AddCert(cert)
		rootsLock.Unlock()
	}
}

func parseCert(certString string) (cert *x509.Certificate) {
	block, _ := pem.Decode([]byte(certString))
	if block == nil {
		log.Error().Str("id", certString).Msg("Failed to decode certificate")
		return
	}
	if block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
		log.Error().Msg("No CERTIFICATE block")
		return
	}
	var err error
	cert, err = x509.ParseCertificate(block.Bytes)
	if err != nil {
		// log.Error().Err(err).Str("id", certString).Msg("Failed to parse certificate")
		return nil
	}
	return
}

func ParseHeader(reader *csv.Reader) map[string]int {
	record, err := reader.Read()
	if err != nil {
		log.Fatal().Err(err).Msg("no csv header given")
	}

	if record == nil || len(record) == 0 {
		log.Fatal().Msg("no csv header given")
	}

	result := make(map[string]int)
	for i, h := range record {
		result[h] = i
	}
	return result
}

func GetSHA256(input []byte) []byte {
	hash := sha256.Sum256(input)
	return hash[:]
}

func GetSHA1(input []byte) []byte {
	hash := sha1.Sum(input)
	return hash[:]
}
