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

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"

namespace starrocks {

class IndexReadOptions;
class PostingIndexPB;
class IndexedColumnReader;
class IndexedColumnIterator;
class BlockPostingIterator;

// Immutable, shareable loader for a term's block posting lists written by BlockPostingWriter.
// load() opens the two IndexedColumns (block column + directory column) once; it holds no mutable
// read state, so a single instance can be cached per-segment and shared across concurrent scans.
// To actually read, mint a per-scan BlockPostingIterator via new_iterator().
// See docs/design/builtin-gin-bm25-storage-pr.md.
class BlockPostingReader {
public:
    BlockPostingReader();
    ~BlockPostingReader();

    BlockPostingReader(const BlockPostingReader&) = delete;
    BlockPostingReader& operator=(const BlockPostingReader&) = delete;

    Status load(const IndexReadOptions& opts, const PostingIndexPB& meta);

    // Create a per-scan cursor over this reader's columns. The returned iterator owns its own
    // IndexedColumnIterators and cursor state; it is NOT thread-safe -- use one per scan. This
    // reader must outlive the iterators it hands out (the segment is pinned for the scan).
    Status new_iterator(const IndexReadOptions& opts, std::unique_ptr<BlockPostingIterator>* out) const;

    // Resident footprint of the two loaded IndexedColumns (for the index mem tracker).
    size_t mem_usage() const;

private:
    std::unique_ptr<IndexedColumnReader> _block_reader;
    std::unique_ptr<IndexedColumnReader> _dir_reader;
};

// Per-scan cursor over one BlockPostingReader's columns. Holds its own IndexedColumnIterators plus
// the mutable position, so concurrent scans each get their own instance and never race. Usage:
//   seek_to_term(ord);                         // load the term's directory
//   while (has_next_block()) { next_block();   // decode the next block
//       for (i in [0, cur_block_size())) use docids()[i] / tfs()[i]; }
// The WAND path additionally uses the per-block max accessors + seek_block to skip blocks.
class BlockPostingIterator {
public:
    BlockPostingIterator(std::unique_ptr<IndexedColumnIterator> block_iter,
                         std::unique_ptr<IndexedColumnIterator> dir_iter);
    ~BlockPostingIterator();

    BlockPostingIterator(const BlockPostingIterator&) = delete;
    BlockPostingIterator& operator=(const BlockPostingIterator&) = delete;

    // Position at a term's posting list (term_ordinal aligned with dict ordinal). Resets block cursor.
    Status seek_to_term(uint32_t term_ordinal);

    // Score-all iteration over the current term's blocks.
    bool has_next_block() const;
    Status next_block();
    size_t cur_block_size() const { return _block_n; }
    const uint32_t* docids() const { return _docids.data(); } // segment rowids of current block
    const uint32_t* tfs() const { return _tfs.data(); }       // term frequencies of current block

    // WAND-facing per-block statistics + block skip (valid only after next_block/seek_block has
    // positioned the cursor on a block; DCHECK guards misuse before the first positioning call).
    // DCHECK catches misuse (reading before next_block/seek_block positions the cursor) in debug;
    // the bounds guard keeps release builds from an out-of-range read when _cur_block == UINT32_MAX.
    uint32_t cur_block_max_tf() const {
        DCHECK_LT(_cur_block, _num_blocks);
        return _cur_block < _num_blocks ? _max_tf[_cur_block] : 0;
    }
    uint32_t cur_block_min_doclen() const {
        DCHECK_LT(_cur_block, _num_blocks);
        return _cur_block < _num_blocks ? _min_doclen[_cur_block] : 0;
    }
    uint32_t cur_block_last_docid() const {
        DCHECK_LT(_cur_block, _num_blocks);
        return _cur_block < _num_blocks ? _last_docid[_cur_block] : 0;
    }
    // Advance to the first block whose last_docid >= target_docid and decode it.
    Status seek_block(uint32_t target_docid);

private:
    Status _load_block(uint32_t block_idx_in_term);
    // Index of the block right after the cursor. Relies on unsigned wraparound of the UINT32_MAX
    // "before first block" sentinel (UINT32_MAX + 1 == 0), so it yields 0 before the first
    // next_block() and _cur_block + 1 afterwards -- no special-case for the sentinel needed.
    uint32_t _next_block_idx() const { return _cur_block + 1; }

    std::unique_ptr<IndexedColumnIterator> _block_iter;
    std::unique_ptr<IndexedColumnIterator> _dir_iter;

    // current term's directory
    uint32_t _num_blocks = 0;
    uint32_t _first_block_id = 0;
    uint32_t _cur_block = UINT32_MAX; // index within current term, [0, _num_blocks)
    std::vector<uint32_t> _last_docid;
    std::vector<uint32_t> _max_tf;
    std::vector<uint32_t> _min_doclen;

    // current decoded block
    std::vector<uint32_t> _docids;
    std::vector<uint32_t> _tfs;
    size_t _block_n = 0;
};

} // namespace starrocks
