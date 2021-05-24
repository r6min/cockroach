// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

var collatedStringType *types.T = types.MakeCollatedString(types.String, `fr`)

var avroBenchmark = &avroEncoderBenchmarkRunner{}
var jsonBenchmark = &jsonEncoderBenchmarkRunner{}

var _ encoderBenchmarkRunner = &avroEncoderBenchmarkRunner{}
var _ encoderBenchmarkRunner = &jsonEncoderBenchmarkRunner{}

type encoderBenchmarkRunner interface {
	benchmarkEncodeType(b *testing.B, typ *types.T, encRow rowenc.EncDatumRow)
}

type avroEncoderBenchmarkRunner struct{}

type jsonEncoderBenchmarkRunner struct {
	enc jsonEncoder
}

func (r *avroEncoderBenchmarkRunner) benchmarkEncodeType(
	b *testing.B, typ *types.T, encRow rowenc.EncDatumRow,
) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	tableDesc, err := parseTableDesc(
		fmt.Sprintf(`CREATE TABLE bench_table (bench_field %s)`, typ.SQLString()))
	require.NoError(b, err)
	schema, err := tableToAvroSchema(tableDesc, "suffix", "namespace")
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := schema.BinaryFromRow(nil, encRow)
		require.NoError(b, err)
	}
}

func (r *jsonEncoderBenchmarkRunner) benchmarkEncodeType(
	b *testing.B, typ *types.T, encRow rowenc.EncDatumRow,
) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)

	tableDesc, err := parseTableDesc(
		fmt.Sprintf(`CREATE TABLE bench_table (bench_field %s)`, typ.SQLString()))
	require.NoError(b, err)
	row := encodeRow{
		datums:        encRow,
		updated:       hlc.Timestamp{},
		tableDesc:     tableDesc,
		prevDatums:    nil,
		prevTableDesc: tableDesc,
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := r.enc.EncodeKey(context.Background(), row)
		require.NoError(b, err)
		_, err = r.enc.EncodeValue(context.Background(), row)
		require.NoError(b, err)
	}
}

// returns random EncDatum row where the first column is of specified
// type and the second one an types.Int, corresponding to a row id.
func randEncDatumRow(typ *types.T) rowenc.EncDatumRow {
	const allowNull = true
	const notNull = false
	rnd, _ := randutil.NewTestPseudoRand()
	return rowenc.EncDatumRow{
		rowenc.DatumToEncDatum(typ, randgen.RandDatum(rnd, typ, allowNull)),
		rowenc.DatumToEncDatum(types.Int, randgen.RandDatum(rnd, types.Int, notNull)),
	}
}
