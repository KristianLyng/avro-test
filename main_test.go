package main
import "testing"

func BenchmarkAvroEncode(b *testing.B) {
	mp := Init()
	for i := 0; i < b.N; i++ {
		EncodeAvro(mp)
	}
}
func BenchmarkAvroDecode(b *testing.B) {
	mp := Init()
	data, err := EncodeAvro(mp)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		DecodeAvro(mp, data)
	}
}
func BenchmarkAvroEncodeGz(b *testing.B) {
	mp := Init()
	for i := 0; i < b.N; i++ {
		EncodeAndGzAvro(mp)
	}
}
func BenchmarkAvroDecodeGz(b *testing.B) {
	mp := Init()
	data, err := EncodeAndGzAvro(mp)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		DecodeAvroGz(mp, data)
	}
}
func BenchmarkJSONEncode(b *testing.B) {
	mp := Init()
	for i := 0; i < b.N; i++ {
		EncodeJSON(mp)
	}
}
func BenchmarkJSONDecode(b *testing.B) {
	mp := Init()
	data, err := EncodeJSON(mp)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		DecodeJSON(mp, data)
	}
}
func BenchmarkJSONEncodeGz(b *testing.B) {
	mp := Init()
	for i := 0; i < b.N; i++ {
		EncodeAndGzJSON(mp)
	}
}
func BenchmarkJSONDecodeGz(b *testing.B) {
	mp := Init()
	data, err := EncodeAndGzJSON(mp)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		DecodeJSONGz(mp, data)
	}
}
func BenchmarkGOBEncode(b *testing.B) {
	mp := Init()
	for i := 0; i < b.N; i++ {
		EncodeGOB(mp)
	}
}
func BenchmarkGOBDecode(b *testing.B) {
	mp := Init()
	data, err := EncodeGOB(mp)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		DecodeGOB(mp, data)
	}
}
func BenchmarkGOBEncodeGz(b *testing.B) {
	mp := Init()
	for i := 0; i < b.N; i++ {
		EncodeAndGzGOB(mp)
	}
}
func BenchmarkGOBDecodeGz(b *testing.B) {
	mp := Init()
	data, err := EncodeAndGzGOB(mp)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		DecodeGOBGz(mp, data)
	}
}
