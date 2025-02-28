package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"

	mitosisbls "github.com/KyrinCode/Mitosis/bls"
	logger "github.com/sirupsen/logrus"
)

func main() {
	cert := make(map[uint32]string)
	for nodeId := uint32(1); nodeId <= 5000; nodeId++ {
		filename := "./keystore/node" + fmt.Sprint(nodeId) + ".keystore"
		secStr, pubStr := mitosisbls.NewKeyPair()
		mitosisbls.SaveKeyPair(filename, secStr, pubStr)
		cert[nodeId] = pubStr
	}

	var prettyJSON bytes.Buffer
	data, err := json.Marshal(cert)
	err = json.Indent(&prettyJSON, data, "", "\t")
	if err != nil {
		logger.Fatal(err)
	}
	fp, err := os.OpenFile("./cert/cert.json", os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		logger.Fatal(err)
	}
	_, err = fp.Write(prettyJSON.Bytes())
	if err != nil {
		log.Fatal(err)
	}
}
