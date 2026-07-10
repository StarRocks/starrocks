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

#include "storage/index/inverted/builtin/block_posting_reader.h"

#include <algorithm>

#include "base/coding.h"
#include "column/chunk_factory.h"
#include "column/column_viewer.h"
#include "gen_cpp/segment.pb.h"
#include "storage/index/inverted/builtin/gin_pfor.h"
#include "storage/rowset/indexed_column_reader.h"
#include "types/logical_type.h"

namespace starrocks {

// Seek the iterator to `ord` and read its single variable-length value into `out` (copied, so it
// stays valid after the scratch column is destroyed).
static Status read_blob_at(IndexedColumnIterator* iter, ordinal_t ord, std::string* out) {
    RETURN_IF_ERROR(iter->seek_to_ordinal(ord));
    // Read the single variable-length value via the same path the bitmap index reader uses for its
    // binary IndexedColumns (ChunkFactory column + ColumnViewer<TYPE_VARCHAR>).
    auto column = ChunkFactory::column_from_field_type(TYPE_VARCHAR, false);
    size_t n = 1;
    RETURN_IF_ERROR(iter->next_batch(&n, column.get()));
    if (n != 1) {
        return Status::Corruption("block posting: short read");
    }
    ColumnViewer<TYPE_VARCHAR> viewer(std::move(column));
    Slice s = viewer.value(0);
    out->assign(s.data, s.size);
    return Status::OK();
}

// ---- BlockPostingReader (immutable loader) --------------------------------------------------

BlockPostingReader::BlockPostingReader() = default;

BlockPostingReader::~BlockPostingReader() = default;

Status BlockPostingReader::load(const IndexReadOptions& opts, const PostingIndexPB& meta) {
    // Only the PFOR block format exists today; a segment written by a future format-version would
    // decode as garbage here, so reject anything else instead of silently mis-decoding.
    if (meta.posting_format() != PostingIndexPB::PFOR) {
        return Status::NotSupported("block posting: unsupported posting_format (only PFOR is supported)");
    }
    _block_reader = std::make_unique<IndexedColumnReader>(meta.posting_block_column());
    _dir_reader = std::make_unique<IndexedColumnReader>(meta.posting_index_column());
    RETURN_IF_ERROR(_block_reader->load(opts));
    RETURN_IF_ERROR(_dir_reader->load(opts));
    return Status::OK();
}

Status BlockPostingReader::new_iterator(const IndexReadOptions& opts,
                                        std::unique_ptr<BlockPostingIterator>* out) const {
    std::unique_ptr<IndexedColumnIterator> block_iter;
    std::unique_ptr<IndexedColumnIterator> dir_iter;
    RETURN_IF_ERROR(_block_reader->new_iterator(opts, &block_iter));
    RETURN_IF_ERROR(_dir_reader->new_iterator(opts, &dir_iter));
    *out = std::make_unique<BlockPostingIterator>(std::move(block_iter), std::move(dir_iter));
    return Status::OK();
}

size_t BlockPostingReader::mem_usage() const {
    size_t size = 0;
    if (_block_reader != nullptr) {
        size += _block_reader->mem_usage();
    }
    if (_dir_reader != nullptr) {
        size += _dir_reader->mem_usage();
    }
    return size;
}

// ---- BlockPostingIterator (per-scan cursor) -------------------------------------------------

BlockPostingIterator::BlockPostingIterator(std::unique_ptr<IndexedColumnIterator> block_iter,
                                           std::unique_ptr<IndexedColumnIterator> dir_iter)
        : _block_iter(std::move(block_iter)), _dir_iter(std::move(dir_iter)) {}

BlockPostingIterator::~BlockPostingIterator() = default;

Status BlockPostingIterator::seek_to_term(uint32_t term_ordinal) {
    std::string blob;
    RETURN_IF_ERROR(read_blob_at(_dir_iter.get(), term_ordinal, &blob));
    if (blob.size() < 8) {
        return Status::Corruption("block posting: directory entry too small");
    }
    const auto* p = reinterpret_cast<const uint8_t*>(blob.data());
    _num_blocks = decode_fixed32_le(p);
    _first_block_id = decode_fixed32_le(p + 4);
    p += 8;
    if (blob.size() < 8 + static_cast<size_t>(_num_blocks) * 12) {
        return Status::Corruption("block posting: directory entry truncated");
    }
    _last_docid.resize(_num_blocks);
    _max_tf.resize(_num_blocks);
    _min_doclen.resize(_num_blocks);
    for (uint32_t i = 0; i < _num_blocks; ++i) {
        _last_docid[i] = decode_fixed32_le(p);
        _max_tf[i] = decode_fixed32_le(p + 4);
        _min_doclen[i] = decode_fixed32_le(p + 8);
        p += 12;
    }
    _cur_block = UINT32_MAX;
    _block_n = 0;
    return Status::OK();
}

bool BlockPostingIterator::has_next_block() const {
    return _next_block_idx() < _num_blocks;
}

Status BlockPostingIterator::next_block() {
    const uint32_t idx = _next_block_idx();
    if (idx >= _num_blocks) {
        // Advancing past the last block would read the next term's block (block ids are a global
        // space), silently returning another term's postings. Reject instead.
        return Status::NotFound("block posting: no more blocks in current term");
    }
    return _load_block(idx);
}

Status BlockPostingIterator::seek_block(uint32_t target_docid) {
    // _last_docid is ascending; lower_bound returns the first block whose last_docid >= target_docid.
    // Search the whole directory (not just from _cur_block forward): WAND drives this monotonically,
    // but a full-range search costs the same O(log n) and stays correct even for a backward target,
    // removing the footgun of silently returning a later block instead of the covering one.
    const auto it = std::lower_bound(_last_docid.begin(), _last_docid.end(), target_docid);
    if (it == _last_docid.end()) {
        return Status::NotFound("block posting: no block covers target docid");
    }
    return _load_block(static_cast<uint32_t>(it - _last_docid.begin()));
}

Status BlockPostingIterator::_load_block(uint32_t block_idx_in_term) {
    std::string blob;
    RETURN_IF_ERROR(read_blob_at(_block_iter.get(), _first_block_id + block_idx_in_term, &blob));
    if (blob.size() < 5) { // doc_count(1) + first_docid(4)
        return Status::Corruption("block posting: block too small");
    }
    const auto* p = reinterpret_cast<const uint8_t*>(blob.data());
    const uint8_t* const end = p + blob.size();
    const uint32_t n = *p++;
    if (n == 0) {
        // The writer never emits an empty block: finish_term() only flushes blocks for non-empty
        // terms (doc_count in [1, kBlockSize]), and an empty term writes zero blocks (num_blocks==0)
        // rather than a zero-count block. A 0 here therefore means a corrupt/truncated blob.
        // (Rejecting it also keeps the n-1 gap count below from underflowing.)
        return Status::Corruption("block posting: block has zero doc_count");
    }
    const uint32_t first_docid = decode_fixed32_le(p);
    p += 4;

    _docids.resize(n);
    _tfs.resize(n);
    std::vector<uint32_t> gaps(n - 1);
    // Even for n == 1 (no gaps) the writer emits a 2-byte empty PFOR header, which decode()
    // consumes and returns non-zero for; a 0 return therefore always means a corrupt/truncated
    // gap stream and must be rejected (rather than silently re-reading these bytes as tfs).
    const size_t c1 = gin_pfor::decode(p, end - p, n - 1, gaps.data());
    if (c1 == 0) {
        return Status::Corruption("block posting: bad gap stream");
    }
    p += c1;
    _docids[0] = first_docid;
    for (uint32_t i = 1; i < n; ++i) {
        _docids[i] = _docids[i - 1] + gaps[i - 1];
    }
    const size_t c2 = gin_pfor::decode(p, end - p, n, _tfs.data());
    if (c2 == 0) {
        return Status::Corruption("block posting: bad tf stream");
    }
    _block_n = n;
    _cur_block = block_idx_in_term;
    return Status::OK();
}

} // namespace starrocks
