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

#include "storage/index/inverted/builtin/builtin_inverted_reader.h"

#include <fmt/format.h>

#include "column/chunk_factory.h"
#include "gen_cpp/segment.pb.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_env.h"
#include "storage/index/index_descriptor.h"
#include "storage/index/inverted/builtin/block_posting_reader.h"
#include "storage/index/inverted/builtin/builtin_inverted_index_iterator.h"
#include "storage/rowset/indexed_column_reader.h"
#include "types/datum.h"
#include "types/logical_type.h"

namespace starrocks {

namespace {
// Read one u32 value at ordinal `ord` from a fixed-width INT IndexedColumn. Both the iterator and the
// `scratch` column are caller-owned and reused across calls within a scan, so a lookup allocates
// nothing: the iterator keeps its page buffer warm and `scratch` is created once (lazily) then cleared
// and refilled per read.
StatusOr<uint32_t> read_u32_at(IndexedColumnIterator* iter, ordinal_t ord, MutableColumnPtr& scratch) {
    RETURN_IF_ERROR(iter->seek_to_ordinal(ord));
    if (scratch == nullptr) {
        scratch = ChunkFactory::column_from_field_type(TYPE_INT, false);
    }
    scratch->reset_column(); // drop the previous row; next_batch appends this read's single value
    size_t n = 1;
    RETURN_IF_ERROR(iter->next_batch(&n, scratch.get()));
    if (n != 1) {
        return Status::Corruption("builtin GIN: short read on u32 column");
    }
    return static_cast<uint32_t>(scratch->get(0).get_int32());
}
} // namespace

BuiltinInvertedReader::BuiltinInvertedReader(const uint32_t index_id, int32_t gram_num)
        : InvertedReader("", index_id), _gram_num(gram_num), _bitmap_index(nullptr) {
    MEM_TRACKER_SAFE_CONSUME(RuntimeEnv::GetInstance()->builtin_inverted_index_mem_tracker(),
                             sizeof(BuiltinInvertedReader));
}

BuiltinInvertedReader::~BuiltinInvertedReader() {
    MEM_TRACKER_SAFE_RELEASE(RuntimeEnv::GetInstance()->builtin_inverted_index_mem_tracker(), mem_usage());
}

size_t BuiltinInvertedReader::mem_usage() const {
    size_t size = sizeof(BuiltinInvertedReader);
    if (_bitmap_index != nullptr) {
        size += _bitmap_index->mem_usage();
    }
    // freqs/norms (DOCS_AND_FREQS) side data loaded in load(); counted so the mem tracker reflects it.
    if (_posting != nullptr) {
        size += _posting->mem_usage();
    }
    if (_doc_freq_reader != nullptr) {
        size += _doc_freq_reader->mem_usage();
    }
    if (_doc_len_reader != nullptr) {
        size += _doc_len_reader->mem_usage();
    }
    return size;
}

Status BuiltinInvertedReader::new_iterator(const std::shared_ptr<TabletIndex> index_meta,
                                           InvertedIndexIterator** iterator, const IndexReadOptions& index_opt) {
    SegmentBitmapIndexIterator* iter;
    RETURN_IF_ERROR(_bitmap_index->new_iterator(index_opt, &iter));
    std::unique_ptr<SegmentBitmapIndexIterator> bitmap_itr;
    bitmap_itr.reset(iter);
    if (!index_opt.segment_rows.has_value()) {
        return Status::InvalidArgument(fmt::format("No segment rows specified"));
    }
    *iterator =
            new BuiltinInvertedIndexIterator(index_meta, this, index_opt.stats, bitmap_itr, *index_opt.segment_rows);
    return Status::OK();
}

Status BuiltinInvertedReader::create(const std::shared_ptr<TabletIndex>& tablet_index, LogicalType field_type,
                                     std::unique_ptr<InvertedReader>* res) {
    if (is_string_type(field_type)) {
        const auto gram_num = get_gram_num_from_properties(tablet_index->index_properties());
        *res = std::make_unique<BuiltinInvertedReader>(tablet_index->index_id(), gram_num);
        return Status::OK();
    } else {
        return Status::InvalidArgument(fmt::format("Not supported type {}", field_type));
    }
}

Status BuiltinInvertedReader::load(const IndexReadOptions& opt, void* meta) {
    if (meta == nullptr) {
        return Status::InvalidArgument("Invalid argument for loading builtin inverted index");
    }
    auto* pb = reinterpret_cast<BuiltinInvertedIndexPB*>(meta);
    const BitmapIndexPB bitmap_index_meta = pb->bitmap_index();
    _bitmap_index = std::make_unique<BitmapIndexReader>(_gram_num, false);

    auto ret = _bitmap_index->load(opt, bitmap_index_meta);
    if (!ret.ok()) {
        _bitmap_index.reset();
        return Status::InternalError(
                fmt::format("Failed to load bitmap index for builtin inverted index: {}", ret.status().to_string()));
    }
    bool first_load = ret.value();
    if (!first_load) {
        _bitmap_index.reset();
        return Status::InternalError("loading builtin inverted index more than once");
    }

    // The two index-options ladders are kept numerically identical; map the persisted proto level into
    // our enum (a new proto level must also be added to InvertedIndexOptions).
    static_assert(static_cast<int>(InvertedIndexOptions::DOCS) == BuiltinInvertedIndexPB::DOCS);
    static_assert(static_cast<int>(InvertedIndexOptions::DOCS_AND_FREQS) == BuiltinInvertedIndexPB::DOCS_AND_FREQS);
    _index_options = static_cast<InvertedIndexOptions>(pb->index_options());

    // freqs/norms storage (present iff index_options >= DOCS_AND_FREQS). Old segments default to DOCS,
    // so this branch is skipped and behavior is unchanged.
    if (has_freqs()) {
        if (Status st = _load_freqs_norms(opt, *pb); !st.ok()) {
            // Side-data load failed after the bitmap was already loaded. Drop every loaded reader so the
            // destructor's mem_usage() release stays symmetric with what was consumed: the ctor consumed
            // only sizeof(*this) and the single consume below never ran on this path, so leaving the
            // partially-loaded readers attached would make the destructor over-release (tracker goes
            // negative).
            _posting.reset();
            _doc_freq_reader.reset();
            _doc_len_reader.reset();
            _bitmap_index.reset();
            return st;
        }
    }

    MEM_TRACKER_SAFE_CONSUME(RuntimeEnv::GetInstance()->builtin_inverted_index_mem_tracker(),
                             mem_usage() - sizeof(BuiltinInvertedReader));
    return Status::OK();
}

Status BuiltinInvertedReader::_load_freqs_norms(const IndexReadOptions& opt, const BuiltinInvertedIndexPB& pb) {
    // A DOCS_AND_FREQS segment must carry the posting + norms metadata and all columns dereferenced
    // below. Reject a partially-written/corrupt segment here with a clear message, rather than failing
    // deep in IndexedColumnReader::load with a generic type/encoding error (which would also abort
    // plain, non-scoring text queries on this column).
    if (!pb.has_posting() || !pb.has_norms() || !pb.posting().has_posting_block_column() ||
        !pb.posting().has_posting_index_column() || !pb.posting().has_doc_freq_column() ||
        !pb.norms().has_doc_len_column()) {
        return Status::Corruption("builtin GIN: DOCS_AND_FREQS segment missing posting/norms metadata");
    }
    _posting = std::make_unique<BlockPostingReader>();
    RETURN_IF_ERROR(_posting->load(opt, pb.posting()));
    _doc_freq_reader = std::make_unique<IndexedColumnReader>(pb.posting().doc_freq_column());
    RETURN_IF_ERROR(_doc_freq_reader->load(opt));
    _doc_len_reader = std::make_unique<IndexedColumnReader>(pb.norms().doc_len_column());
    RETURN_IF_ERROR(_doc_len_reader->load(opt));
    _sum_len = pb.norms().sum_len();
    return Status::OK();
}

StatusOr<std::unique_ptr<FreqsIterator>> BuiltinInvertedReader::new_freqs_iterator(const IndexReadOptions& opts) const {
    if (!has_freqs()) {
        return Status::InternalError("builtin GIN: index has no freqs/norms (index_options is DOCS)");
    }
    std::unique_ptr<IndexedColumnIterator> df_iter;
    std::unique_ptr<IndexedColumnIterator> doc_len_iter;
    RETURN_IF_ERROR(_doc_freq_reader->new_iterator(opts, &df_iter));
    RETURN_IF_ERROR(_doc_len_reader->new_iterator(opts, &doc_len_iter));
    return std::make_unique<FreqsIterator>(_posting.get(), std::move(df_iter), std::move(doc_len_iter), _sum_len);
}

FreqsIterator::FreqsIterator(const BlockPostingReader* posting_loader, std::unique_ptr<IndexedColumnIterator> df_iter,
                             std::unique_ptr<IndexedColumnIterator> doc_len_iter, uint64_t sum_len)
        : _posting_loader(posting_loader),
          _df_iter(std::move(df_iter)),
          _doc_len_iter(std::move(doc_len_iter)),
          _sum_len(sum_len) {}

FreqsIterator::~FreqsIterator() = default;

Status FreqsIterator::new_posting_cursor(const IndexReadOptions& opts, std::unique_ptr<BlockPostingIterator>* out) {
    return _posting_loader->new_iterator(opts, out);
}

StatusOr<uint32_t> FreqsIterator::doc_freq(uint32_t term_ordinal) {
    return read_u32_at(_df_iter.get(), term_ordinal, _u32_scratch);
}

StatusOr<uint32_t> FreqsIterator::doc_len(rowid_t rid) {
    return read_u32_at(_doc_len_iter.get(), rid, _u32_scratch);
}

} // namespace starrocks
