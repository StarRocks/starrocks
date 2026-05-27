// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "column/vectorized_fwd.h"
#include "storage/file_stream_converter.h"
#include "storage/primitive/primary_key_encoding_types.h"
#include "storage/types.h"

namespace starrocks {

class Schema;
class TabletSchema;
using SchemaPtr = std::shared_ptr<Schema>;

// Returns true iff the primary key is a single non-string fixed-length column -- the only
// PK shape where V1 and V2 produce different on-disk .del layouts. V1 stores it as a typed
// column (e.g. Int32Column); V2 stores it as a BinaryColumn with order-preserving big-endian
// slices. Composite keys and single-VARCHAR PK both materialize as BinaryColumn with
// identical layout in V1 and V2.
bool is_single_fixed_length_non_string_primary_key(const Schema& pkey_schema);
bool is_single_fixed_length_non_string_primary_key(const TabletSchema& tablet_schema);

// Returns true iff a .del file written with source PK encoding must be re-encoded to be
// safely read under target PK encoding. Only V1 -> V2 on the byte-incompatible PK shape
// above is supported for transcoding. The reverse direction (V2 -> V1) is byte-incompatible
// too but is not transcodable here (no V2 -> typed-column decoder); callers should reject
// it explicitly upstream.
bool requires_v1_to_v2_del_transcode(PrimaryKeyEncodingType source_encoding, PrimaryKeyEncodingType target_encoding,
                                     const Schema& pkey_schema);

// Transcodes a primary-key .del file from source PK encoding to target PK encoding
// during cross-cluster replication intake.
//
// The .del file holds the serialized PK column for deleted rows, written via
// `serde::ColumnArraySerde::serialize`. When the source uses V1 with a single
// non-string fixed-length PK, the on-disk shape is a typed column (e.g. Int32Column).
// When the target uses V2, all encoders/readers expect a BinaryColumn whose slices
// are order-preserving big-endian encodings. The two shapes are not byte-compatible,
// so the bytes must be re-encoded once at intake. After this converter writes the
// output file, every downstream consumer (publish-time `load_delete`, persistent-index
// `load_dels`, future compaction or readers) sees target-encoded bytes uniformly.
//
// This converter is intentionally narrow: it is invoked ONLY when
//   source = V1, target = V2, num_key_columns == 1, and the single PK column is one
//   of the non-string fixed-length types listed in PrimaryKeyEncoder::is_supported
//   (BOOLEAN/TINYINT/SMALLINT/INT/BIGINT/LARGEINT/DATE/DATETIME).
// All other shapes are byte-identical between V1 and V2 (composite keys and single
// VARCHAR PK both materialize as BinaryColumn with identical layout) and are handled
// by the plain FileStreamConverter base.
//
// Buffering: a .del file is the serialized delete column from one memtable flush.
// The full payload is buffered before decoding (the ColumnArraySerde format is not
// streamable). This matches the existing RowsetUpdateState::load_delete pattern,
// which also read_all()s the entire .del file before deserializing.
class DelFileStreamConverter : public FileStreamConverter {
public:
    DelFileStreamConverter(std::string_view input_file_name, uint64_t input_file_size,
                           std::unique_ptr<WritableFile> output_file, SchemaPtr pkey_schema,
                           PrimaryKeyEncodingType source_pk_encoding, PrimaryKeyEncodingType target_pk_encoding);

    Status append(const void* data, size_t size) override;
    Status close() override;

private:
    SchemaPtr _pkey_schema;
    PrimaryKeyEncodingType _source_pk_encoding;
    PrimaryKeyEncodingType _target_pk_encoding;
    std::string _input_buffer;
};

} // namespace starrocks
