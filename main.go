package main

import (
	"fmt"
	"log"
	"io"
	"time"
	"os"

	"bytes"
	"encoding/json"
	"encoding/gob"
	"compress/gzip"
	"math/rand"

	"github.com/telenornms/skogul"
	"github.com/hamba/avro"
)

// Number of metrics in a generated container
const NumMetrics = 1000

func printLol(what string, b []byte) {
	fmt.Printf("%20s length %d - %d metrics, %d bytes per metric\n", what, len(b), NumMetrics, len(b)/NumMetrics)
}

func compressIt(b []byte) ([]byte) {
	var buf bytes.Buffer
	w,err := gzip.NewWriterLevel(&buf, 9)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	w.Write(b)
	w.Close()
	return buf.Bytes()
}

func decompressIt(b []byte) ([]byte) {
	var buf bytes.Buffer
	buf.Write(b)
	w,err := gzip.NewReader(&buf)
	if err != nil {
		log.Fatal(err)
		return nil
	}
	dec,err := io.ReadAll(w)
	w.Close()
	return dec
}

func MakeMetric() *skogul.Metric {
	m := skogul.Metric{}
	now := time.Now()
	m.Time = &now
	m.Metadata = make(map[string]interface{})
	m.Data = make(map[string]interface{})

	m.Metadata["band_tag"] = "3"
	m.Metadata["carrier"] = "4g"
	m.Metadata["cell_id"] = "5149123"
	m.Metadata["event_type"] = "cellular"
	m.Metadata["imei"] = "13124125123"
	m.Metadata["serial_number"] = "S121Z1231"
	m.Data["band"] = "5g"
	m.Data["cell_id"] = rand.Float64()
	m.Data["cqi"] = rand.Float64()
	m.Data["dl_bw"] = rand.Float64()
	m.Data["earfcn"] = rand.Float64()
	m.Data["mcs"] = rand.Float64()
	m.Data["phy_cell_id"] = rand.Float64()
	m.Data["pmi"] = rand.Float64()
	m.Data["ri"] = rand.Float64()
	m.Data["rsrp"] = rand.Float64()
	m.Data["rsrq"] = rand.Float64()
	m.Data["rssi"] = rand.Float64()
	m.Data["sinr"] = rand.Float64()
	m.Data["txpower"] = rand.Float64()
	m.Data["ul_bw"] = rand.Float64()
	return &m
}

type Prep struct {
	Schema avro.Schema
	In	skogul.Container
}

func Init() Prep {
	var P Prep
	b, err := os.ReadFile("schema")
	if err != nil {
		log.Fatal(err)
	}
	P.Schema = avro.MustParse(string(b))

	for i := 0; i < NumMetrics; i++ {
		P.In.Metrics = append(P.In.Metrics, MakeMetric())
	}
	return P
}

func EncodeAvro(p Prep) ([]byte, error) {
	return avro.Marshal(p.Schema, p.In)
}

func DecodeAvro(p Prep, b []byte) (skogul.Container, error) {
	var c skogul.Container
	err := avro.Unmarshal(p.Schema, b, &c)
	return c, err
}

func EncodeAndGzAvro(p Prep) ([]byte, error) {
	data, err := EncodeAvro(p)
	if err != nil {
		log.Fatal(err)
	}
	return compressIt(data),nil
}

func DecodeAvroGz(p Prep, b []byte) (skogul.Container, error) {
	nb := decompressIt(b)
	return DecodeAvro(p, nb)
}

func DecodeJSON(p Prep, b []byte) (skogul.Container, error) {
	var c skogul.Container
	err := json.Unmarshal(b, &c)
	return c, err
}

func EncodeJSON(p Prep) ([]byte, error) {
	return json.Marshal(p.In)
}

func DecodeJSONGz(p Prep, b []byte) (skogul.Container, error) {
	nb := decompressIt(b)
	return DecodeJSON(p, nb)
}

func EncodeAndGzJSON(p Prep) ([]byte, error) {
	b, err := json.Marshal(p.In)
	if err != nil {
		log.Fatal(err)
	}
	return compressIt(b), nil
}

func EncodeGOB(p Prep) ([]byte, error) {
	var network bytes.Buffer
	enc := gob.NewEncoder(&network)
	err := enc.Encode(p.In)
	return network.Bytes(), err
}

func EncodeAndGzGOB(p Prep) ([]byte, error) {
	b,err := EncodeGOB(p)
	if err != nil {
		return nil, err
	}
	return compressIt(b), nil
}

func DecodeGOB(p Prep, b []byte) (skogul.Container, error) {
	var buf bytes.Buffer
	buf.Write(b)
	w := gob.NewDecoder(&buf)
	var c skogul.Container
	err := w.Decode(&c)
	if err != nil {
		log.Fatal(err)
	}
	return c, err
}

func DecodeGOBGz(p Prep, b []byte) (skogul.Container, error) {
	nb := decompressIt(b)
	return DecodeGOB(p, nb)
}

func main() {
	p := Init()
	data, err := EncodeAvro(p)
	if err != nil {
		log.Fatal(err)
	}
	printLol("avro uncompressed", data)
	_, err = DecodeAvro(p, data)
	if err != nil {
		log.Fatal(err)
	}
	az,err := EncodeAndGzAvro(p)
	if err != nil {
		log.Fatal(err)
	}
	printLol("avro compressed", az)
	
	b, err := EncodeJSON(p)
	if err != nil {
		log.Fatal(err)
	}
	printLol("json uncompressed", b)

	zb,err := EncodeAndGzJSON(p)
	if err != nil {
		log.Fatal(err)
	}
	printLol("json compressed", zb)
	DecodeJSONGz(p, zb)

	gb, err := EncodeGOB(p)
	if err != nil {
		log.Fatal(err)
	}
	printLol("gob uncompressed", gb)
	gzb, err := EncodeAndGzGOB(p)
	if err != nil {
		log.Fatal(err)
	}
	printLol("gob compressed", gzb)
}
