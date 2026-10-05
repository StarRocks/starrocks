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
#include <string>
#include <vector>

#include "column/global_dict/types.h"
#include "column/variant_path_parser.h"
#include "formats/parquet/column_reader.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {

struct VariantShreddedReadHints {
    // String form of the paths, kept in sync with parsed_shredded_paths via add_path().
    // Used for string-level deduplication during hint collection (see _get_variant_shredded_hints).
    // Not forwarded to column readers; only parsed_shredded_paths is passed downstream.
    std::vector<std::string> shredded_paths;
    // Parsed form of shredded_paths, kept in the same order.
    // This is what column readers consume for segment-level path pruning.
    // Empty means no restriction: the reader auto-discovers paths from the shredded schema.
    std::vector<VariantPath> parsed_shredded_paths;

    // Appends a path in both string and parsed form, keeping the two vectors in sync.
    Status add_path(std::string path);
    void clear();
};

// Which dict-aware wrapper a global-dict-encoded slot ended up with. Reported back
// by the dict-wrapping create() overload so the GroupReader can attribute the
// applied slot to the right counter (Parquet has no force-encode fallback like
// OLAP; the two truthful paths are LowCardColumnReader for all-page-dict columns
// and LowRowsColumnReader when row count fits the FE dictionary).
enum class GlobalDictReaderKind {
    kNone,          // no wrapper selected (factory returned GlobalDictNotMatch)
    kDictCode,      // LowCardColumnReader: reader consumes Parquet dict codes directly
    kLowRowsEncode, // LowRowsColumnReader: reader reads strings then encodes per row
};

// How the children of a struct column type map onto a parquet struct (or variant) group. Indexed by the child
// position in the column type.
struct StructSubfieldMapping {
    // The parquet child read for the type child, nullptr if the file does not have it (read as null).
    std::vector<const ParquetField*> parquet_children;
    // The Iceberg schema child of the type child; only filled on the Iceberg path (else nullptr).
    std::vector<const TIcebergSchemaField*> lake_children;
};

class ColumnReaderFactory {
public:
    // create a column reader
    static StatusOr<ColumnReaderPtr> create(const ColumnReaderOptions& opts, const ParquetField* field,
                                            const TypeDescriptor& col_type);

    // Create a column reader with iceberg schema. lake_schema_field == nullptr is the same as the overload above.
    static StatusOr<ColumnReaderPtr> create(const ColumnReaderOptions& opts, const ParquetField* field,
                                            const TypeDescriptor& col_type,
                                            const TIcebergSchemaField* lake_schema_field);

    // Wrap an already-built scalar reader with a dict-aware reader for the given
    // FE global dictionary.  If out_kind is non-null, it is set to identify which
    // wrapper was picked so the caller can publish profile counters for the
    // dict-code vs. low-rows-encode path.  Out_kind is left at kNone on failure
    // (the function returns a GlobalDictNotMatch status in that case).
    static StatusOr<ColumnReaderPtr> create(ColumnReaderPtr raw_reader, const GlobalDictMap* dict, const SlotId slot_id,
                                            int64_t num_rows, GlobalDictReaderKind* out_kind = nullptr);

    static StatusOr<ColumnReaderPtr> create_variant_column_reader(const ColumnReaderOptions& opts,
                                                                  const ParquetField* variant_field,
                                                                  const VariantShreddedReadHints& hints = {});

    // Resolve which parquet child each child of the struct column type `col_type` reads. The single matching rule
    // used by the reader factory and the meta helpers:
    // - Iceberg path (lake_schema_field != nullptr): the Iceberg child by table field name; then the parquet child
    //   by its Iceberg field id if the file has field ids, else by physical name (if any) or table field name.
    // - Otherwise: by field id if the type carries field ids, else by physical name, else by field name. An entry
    //   without an id (-1) or physical name ("") falls back to the field name.
    // Names are compared after Utils::format_name(case_sensitive).
    static StructSubfieldMapping resolve_struct_subfields(const ParquetField& field, const TypeDescriptor& col_type,
                                                          bool case_sensitive,
                                                          const TIcebergSchemaField* lake_schema_field,
                                                          bool parquet_has_field_id);
};

} // namespace starrocks::parquet
