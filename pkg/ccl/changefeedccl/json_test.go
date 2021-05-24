// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
)

func BenchmarkJSONEncodeIntArray(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.IntArray, randEncDatumRow(types.IntArray))
}

func BenchmarkJSONEncodeInt(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Int, randEncDatumRow(types.Int))
}

func BenchmarkJSONEncodeBool(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Bool, randEncDatumRow(types.Bool))
}

func BenchmarkJSONEncodeFloat(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Float, randEncDatumRow(types.Float))
}

func BenchmarkJSONEncodeBox2D(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Box2D, randEncDatumRow(types.Box2D))
}

func BenchmarkJSONEncodeGeography(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Geography, randEncDatumRow(types.Geography))
}

func BenchmarkJSONEncodeGeometry(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Geometry, randEncDatumRow(types.Geometry))
}

func BenchmarkJSONEncodeBytes(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Bytes, randEncDatumRow(types.Bytes))
}

func BenchmarkJSONEncodeString(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.String, randEncDatumRow(types.String))
}

func BenchmarkJSONEncodeCollatedString(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, collatedStringType, randEncDatumRow(collatedStringType))
}

func BenchmarkJSONEncodeDate(b *testing.B) {
	// RandDatum could return "interesting" dates (infinite past, etc).  Alas, avro
	// doesn't support those yet, so override it to something we do support.
	encRow := randEncDatumRow(types.Date)
	if d, ok := encRow[0].Datum.(*tree.DDate); ok && !d.IsFinite() {
		d.Date = pgdate.LowDate
	}
	jsonBenchmark.benchmarkEncodeType(b, types.Date, encRow)
}

func BenchmarkJSONEncodeTime(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Time, randEncDatumRow(types.Time))
}

func BenchmarkJSONEncodeTimeTZ(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.TimeTZ, randEncDatumRow(types.TimeTZ))
}

func BenchmarkJSONEncodeTimestamp(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Timestamp, randEncDatumRow(types.Timestamp))
}

func BenchmarkJSONEncodeTimestampTZ(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.TimestampTZ, randEncDatumRow(types.TimestampTZ))
}

func BenchmarkJSONEncodeDecimal(b *testing.B) {
	typ := types.MakeDecimal(10, 4)
	encRow := randEncDatumRow(typ)

	// rowenc.RandDatum generates all possible datums. We just want small subset
	// to fit in our specified precision/scale.
	d := &tree.DDecimal{}
	coeff := int64(rand.Uint64()) % 10000
	d.Decimal.SetFinite(coeff, 2)
	encRow[0] = rowenc.DatumToEncDatum(typ, d)
	jsonBenchmark.benchmarkEncodeType(b, typ, encRow)
}

func BenchmarkJSONEncodeUUID(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Uuid, randEncDatumRow(types.Uuid))
}

func BenchmarkJSONEncodeINet(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.INet, randEncDatumRow(types.INet))
}

func BenchmarkJSONEncodeJSON(b *testing.B) {
	jsonBenchmark.benchmarkEncodeType(b, types.Jsonb, randEncDatumRow(types.Jsonb))
}
