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
#include <vector>

#include "base/string/faststring.h"
#include "common/status.h"

namespace starrocks {

class WritableFile;
class IndexedColumnWriter;
class IndexedColumnMetaPB;

// Encodes per-term (docid, tf) posting lists into fixed-size (128-doc) blocks, plus a directory
// with per-block {last_docid, max_tf, min_doclen} (the WAND block-max statistics). Produces two
// IndexedColumns written inline into the segment .dat:
//   - block column     : one variable-length entry per block, keyed by global block id (ordinal)
//   - directory column : one variable-length entry per term, keyed by dict ordinal
// Block payloads use the architecture-neutral PFOR codec (gin_pfor) for docid gaps and tfs.
// See docs/design/builtin-gin-bm25-storage-pr.md.
class BlockPostingWriter {
public:
    static constexpr uint32_t kBlockSize = 128;

    explicit BlockPostingWriter(WritableFile* wfile);
    ~BlockPostingWriter();

    BlockPostingWriter(const BlockPostingWriter&) = delete;
    BlockPostingWriter& operator=(const BlockPostingWriter&) = delete;

    Status init();

    // Begin a new term's posting list. Terms must be fed in dict-ordinal order so that the
    // directory column's ordinal i corresponds to dict ordinal i.
    void start_term(uint32_t term_ordinal);
    // Append one posting (docids must be added in increasing order). doc_len feeds the per-block
    // min_doclen used for the WAND upper bound.
    void add(uint32_t docid, uint32_t tf, uint32_t doc_len);
    // Flush the current term's blocks and write its directory entry.
    Status finish_term();
    // Finalize both IndexedColumns and populate their metadata.
    Status finish(IndexedColumnMetaPB* block_meta, IndexedColumnMetaPB* dir_meta);

private:
    Status _flush_block(size_t base, size_t n, uint32_t* last_docid, uint32_t* max_tf, uint32_t* min_doclen);

    WritableFile* _wfile;
    std::unique_ptr<IndexedColumnWriter> _block_col;
    std::unique_ptr<IndexedColumnWriter> _dir_col;

    // accumulation for the current term
    std::vector<uint32_t> _docids;
    std::vector<uint32_t> _tfs;
    std::vector<uint32_t> _doclens;

    uint32_t _next_block_id = 0;
    faststring _block_buf;
    faststring _dir_buf;
};

} // namespace starrocks
