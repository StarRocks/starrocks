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

#include <memory>
#include <string>

#include "common/status.h"
#include "fs/fs.h"
#include "storage/index/inverted/inverted_reader.h"
#include "storage/index/inverted/tantivy/tantivy_ffi_guards.h"
#include "storage/tablet_index.h"

namespace starrocks {

class TantivyInvertedReader : public InvertedReader {
public:
    static Status create(const std::string& path, const std::shared_ptr<TabletIndex>& tablet_index,
                         LogicalType field_type, std::unique_ptr<InvertedReader>* res);

    static Status open_compound(TantivyInvertedReader* reader, FileSystem* fs,
                                const std::string& bin_path, int64_t index_id,
                                const std::string& column_name);

    TantivyInvertedReader(std::string path, uint32_t index_id, std::string field_name, std::string tokenizer_name);
    ~TantivyInvertedReader() override = default;

    Status load(const IndexReadOptions& opt, void* meta) override;

    // Load from a compound .idx file instead of a local directory.
    // Takes ownership of ra_file (must outlive the Rust compound reader).
    Status load_compound(std::unique_ptr<RandomAccessFile> ra_file, const std::string& file_table_json);

    void set_null_bitmap(roaring::Roaring bitmap) { _null_bitmap = std::move(bitmap); }
    void set_field_name(std::string name) { _field_name = std::move(name); }

    Status new_iterator(const std::shared_ptr<TabletIndex> index_meta, InvertedIndexIterator** iterator,
                        const IndexReadOptions& index_opt) override;

    Status query(OlapReaderStatistics* stats, const std::string& column_name, const void* query_value,
                 InvertedIndexQueryType query_type, roaring::Roaring* bit_map) override;

    Status query_scored(OlapReaderStatistics* stats, const std::string& column_name, const void* query_value,
                        InvertedIndexQueryType query_type, int32_t limit, float min_score, float max_score,
                        roaring::Roaring* bit_map, std::unordered_map<uint32_t, float>* row_to_score) override;

    Status query_null(OlapReaderStatistics* stats, const std::string& column_name,
                      roaring::Roaring* bit_map) override;

    InvertedIndexReaderType get_inverted_index_reader_type() override;

private:
    // Single dispatch path for all query types. The local- vs compound-load
    // branch in `query()` is now just "pick the underlying reader handle";
    // tokenization, FFI invocation, error handling, and result conversion are
    // identical.
    Status _query_impl(void* reader_handle, const void* query_value, InvertedIndexQueryType query_type,
                       roaring::Roaring* bit_map);

    // Scored dispatch: runs a BM25-scoring tantivy query (MATCH_ANY/MATCH_ALL),
    // fills `bit_map` with matched rows AND `row_to_score` with their scores.
    Status _query_impl_scored(void* reader_handle, const void* query_value, InvertedIndexQueryType query_type,
                              int32_t limit, float min_score, float max_score, roaring::Roaring* bit_map,
                              std::unordered_map<uint32_t, float>* row_to_score);

    std::string _field_name;
    std::string _tokenizer_name;

    // Local reader (direct .ivt directory).
    TantivyReaderGuard _reader;

    // Compound reader (reads from .idx via PullDirectory FFI).
    TantivyCompoundReaderGuard _compound_reader;
    std::unique_ptr<RandomAccessFile> _compound_ra_file;

    roaring::Roaring _null_bitmap;
    bool _loaded = false;
    bool _is_compound = false;
};

} // namespace starrocks
