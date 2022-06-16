/*
 * avro-test - various encoder/decoder tests
 *
 * Copyright (c) 2022 Telenor Norge AS
 * Author(s):
 *  - Kristian Lyngstøl <kly@kly.no>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301  USA
 */

/*
Package main / avro-test facilitates various benchmarks of naive
implementations of various encoders and decoders. They are naive in the
sense that they do not re-use encoder-objects, which particularly for
GOB, introduces a very significant overhead.

The tests are meant to benchmark the encodings for use in a setting where
they need to be completely isolated from the transport layer, which means
things like disconnects are invisible to the enocder, thus the encoder
can't make an intelligent decision on when to re-send headers.

The tests come in two version: Pure benchmark in the style of go testing,
to run them use:

	go test -bench=.*

The other type of tests measure the size of the encoded data, run them by
building the package and running the regular binary.

	go build ./
	./avro-test

Again, these are naive and trivial tests - do not put too much value in
them beyond these very specific use cases.
*/
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

// MakeMetric generates a skogul.Metric with some quasi-reasonable setup.
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

// Prep contains the parsed avro schema and the container to test
type Prep struct {
	Schema avro.Schema
	In	skogul.Container
}

// Init generates the container for testing and reads the avro schema.
// Needs to be separate from main() because the test cases/benchmarks needs
// it too.
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
