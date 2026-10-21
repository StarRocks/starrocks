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
#include "formats/parquet/column_reader.h"

namespace starrocks::parquet {

<<<<<<< HEAD
=======
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
    // The Iceberg schema child of the type child, also when the file lacks it; only filled on the Iceberg path
    // (else nullptr).
    std::vector<const TIcebergSchemaField*> lake_children;
};

>>>>>>> d82b955 ([BugFix] Fix Iceberg read correctness: unknown transforms, ORC field ids, promoted bounds, unknown type, default values (#81257))
class ColumnReaderFactory {
public:
    // create a column reader
    static StatusOr<ColumnReaderPtr> create(const ColumnReaderOptions& opts, const ParquetField* field,
                                            const TypeDescriptor& col_type);

    // Create a column reader with iceberg schema
    static StatusOr<ColumnReaderPtr> create(const ColumnReaderOptions& opts, const ParquetField* field,
                                            const TypeDescriptor& col_type,
                                            const TIcebergSchemaField* lake_schema_field);

    static StatusOr<ColumnReaderPtr> create(ColumnReaderPtr raw_reader, const GlobalDictMap* dict, const SlotId slot_id,
                                            int64_t num_rows);

private:
    // for struct type without schema change
    static void get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                  bool case_sensitive, std::vector<int32_t>& pos);

    // for schema changed
    static void get_subfield_pos_with_pruned_type(const ParquetField& field, const TypeDescriptor& col_type,
                                                  bool case_sensitive, const TIcebergSchemaField* lake_schema_field,
                                                  std::vector<int32_t>& pos,
                                                  std::vector<const TIcebergSchemaField*>& lake_schema_subfield);

    static bool _has_valid_subfield_column_reader(
            const std::map<std::string, std::unique_ptr<ColumnReader>>& children_readers);

    static StatusOr<ColumnReaderPtr> create_variant_column_reader(const ColumnReaderOptions& opts,
                                                                  const ParquetField* variant_field);
};

} // namespace starrocks::parquet