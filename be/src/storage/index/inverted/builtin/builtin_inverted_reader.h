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

#include <CLucene.h>

#include <utility>

#include "column/column.h"
#include "common/statusor.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_reader.h"
#include "storage/rowset/bitmap_index_reader.h"
#include "storage_primitive/rowid_types.h"

namespace starrocks {

class InvertedIndexIterator;
enum class InvertedIndexQueryType;
enum class InvertedIndexReaderType;
class IndexReadOptions;
class FunctionContext;
class BlockPostingReader;
class BlockPostingIterator;
class IndexedColumnReader;
class IndexedColumnIterator;
class FreqsIterator;
class BuiltinInvertedIndexPB;

class BuiltinInvertedReader : public InvertedReader {
public:
    explicit BuiltinInvertedReader(const uint32_t index_id, int32_t gram_num);
    ~BuiltinInvertedReader() override;

    static Status create(const std::shared_ptr<TabletIndex>& tablet_index, LogicalType field_type,
                         std::unique_ptr<InvertedReader>* res);

    Status new_iterator(const std::shared_ptr<TabletIndex> index_meta, InvertedIndexIterator** iterator,
                        const IndexReadOptions& index_opt) override;

    Status load(const IndexReadOptions& opt, void* meta) override;

    // Includes the bitmap index plus, on a DOCS_AND_FREQS segment, the freqs/norms posting/doc_freq/doc_len
    // readers loaded in load(). Defined out-of-line because those readers are incomplete types here.
    size_t mem_usage() const;

    /*
       Implemented in BuiltinInvertedIndexIterator. For builtin inverted index, we have to define bitmap index iterator in BuiltinInvertedIndexIterator.
       Because BuiltinInvertedReader may be accessed by multiple threads, and bitmap index iterator is not thread-safe.
    */
    Status query(OlapReaderStatistics* stats, const std::string_view column_name, const void* query_value,
                 InvertedIndexQueryType query_type, roaring::Roaring* bit_map) override {
        return Status::InternalError("Unreachable");
    }

    Status query_null(OlapReaderStatistics* stats, const std::string_view column_name,
                      roaring::Roaring* bit_map) override {
        return Status::InternalError("Unreachable");
    }

    InvertedIndexReaderType get_inverted_index_reader_type() override { return InvertedIndexReaderType::TEXT; }

    // ---- freqs/norms (>= DOCS_AND_FREQS) accessors. Valid only when has_freqs(); read by the scoring layer. ----
    // Whether this segment's index carries term frequencies + norms (index_options >= DOCS_AND_FREQS).
    bool has_freqs() const { return _index_options >= InvertedIndexOptions::DOCS_AND_FREQS; }
    // Mint a per-scan freqs/norms read handle (one per scan; NOT thread-safe): it owns reused doc_freq /
    // doc_len iterators and a factory for per-term posting cursors, while this shared reader stays
    // immutable. Fails if the index has no freqs (index_options is DOCS).
    StatusOr<std::unique_ptr<FreqsIterator>> new_freqs_iterator(const IndexReadOptions& opts) const;

private:
    // Load the freqs/norms side data (posting + doc_freq + doc_len + sum_len) for a >= DOCS_AND_FREQS
    // segment. On failure the caller drops every loaded reader so the mem-tracker consume/release stays
    // symmetric (the single consume in load() only runs once this returns OK).
    Status _load_freqs_norms(const IndexReadOptions& opt, const BuiltinInvertedIndexPB& pb);

    int32_t _gram_num;
    std::unique_ptr<BitmapIndexReader> _bitmap_index;

    // Parsed index_options level for this segment (default DOCS). freqs/norms side data below is present only
    // when it is >= DOCS_AND_FREQS. All immutable after load() (shared, read-only).
    InvertedIndexOptions _index_options = InvertedIndexOptions::DOCS;
    std::unique_ptr<BlockPostingReader> _posting; // immutable loader; per-scan cursors via new_freqs_iterator
    std::unique_ptr<IndexedColumnReader> _doc_len_reader;
    std::unique_ptr<IndexedColumnReader> _doc_freq_reader;
    uint64_t _sum_len = 0;
};

// Per-scan freqs/norms read handle (one per scan; NOT thread-safe). Holds reused doc_freq / doc_len column
// iterators and mints per-term posting cursors from the shared (immutable) BlockPostingReader. This
// keeps all mutable read state off the per-segment-cached BuiltinInvertedReader, so concurrent scans
// never race (each holds its own FreqsIterator).
class FreqsIterator {
public:
    FreqsIterator(const BlockPostingReader* posting_loader, std::unique_ptr<IndexedColumnIterator> df_iter,
                  std::unique_ptr<IndexedColumnIterator> doc_len_iter, uint64_t sum_len);
    ~FreqsIterator();

    FreqsIterator(const FreqsIterator&) = delete;
    FreqsIterator& operator=(const FreqsIterator&) = delete;

    // Open a term's posting list (lazy; one cursor per query term). term_ordinal is dict-aligned.
    Status new_posting_cursor(const IndexReadOptions& opts, std::unique_ptr<BlockPostingIterator>* out);
    // Document frequency of the term at dict ordinal `term_ordinal` (reuses the df iterator).
    StatusOr<uint32_t> doc_freq(uint32_t term_ordinal);
    // Document length (token count) of row `rid` (reuses the doc_len iterator).
    StatusOr<uint32_t> doc_len(rowid_t rid);
    // Sum of doc_len over all rows in this segment (for tablet-level avgdl).
    uint64_t sum_len() const { return _sum_len; }

private:
    const BlockPostingReader* _posting_loader;
    std::unique_ptr<IndexedColumnIterator> _df_iter;
    std::unique_ptr<IndexedColumnIterator> _doc_len_iter;
    uint64_t _sum_len;
    // Reused single-row TYPE_INT buffer for doc_freq()/doc_len() point reads (both are u32 columns),
    // so a scan does not allocate a fresh column per lookup. Cleared before each read.
    MutableColumnPtr _u32_scratch;
};

} // namespace starrocks
