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

#include "storage/index/inverted/builtin/block_posting_writer.h"

#include <algorithm>

#include "base/coding.h"
#include "base/string/slice.h"
#include "gen_cpp/segment.pb.h"
#include "storage/index/inverted/builtin/gin_pfor.h"
#include "storage/rowset/encoding_info.h"
#include "storage/rowset/indexed_column_writer.h"
#include "storage/types.h"
#include "types/logical_type.h"

namespace starrocks {

BlockPostingWriter::BlockPostingWriter(WritableFile* wfile) : _wfile(wfile) {}

BlockPostingWriter::~BlockPostingWriter() = default;

Status BlockPostingWriter::init() {
    TypeInfoPtr typeinfo = get_type_info(TYPE_VARCHAR);
    IndexedColumnWriterOptions options;
    options.write_ordinal_index = true; // seek by block id / term ordinal
    options.write_value_index = false;
    // PLAIN (not the VARCHAR default DICT_ENCODING): these are opaque byte blobs read by ordinal,
    // and dict-encoded pages cannot be random-accessed on the seek_to_ordinal path.
    options.encoding = PLAIN_ENCODING;
    // Block payloads are already PFOR-packed; the directory is small and point-read. NO_COMPRESSION
    // avoids a whole-page decompress per point lookup.
    options.compression = NO_COMPRESSION;

    _block_col = std::make_unique<IndexedColumnWriter>(options, typeinfo, _wfile);
    RETURN_IF_ERROR(_block_col->init());
    _dir_col = std::make_unique<IndexedColumnWriter>(options, typeinfo, _wfile);
    RETURN_IF_ERROR(_dir_col->init());
    return Status::OK();
}

void BlockPostingWriter::start_term(uint32_t /*term_ordinal*/) {
    _docids.clear();
    _tfs.clear();
    _doclens.clear();
}

void BlockPostingWriter::add(uint32_t docid, uint32_t tf, uint32_t doc_len) {
    _docids.push_back(docid);
    _tfs.push_back(tf);
    _doclens.push_back(doc_len);
}

Status BlockPostingWriter::_flush_block(size_t base, size_t n, uint32_t* last_docid, uint32_t* max_tf,
                                        uint32_t* min_doclen) {
    *last_docid = _docids[base + n - 1];
    *max_tf = 0;
    *min_doclen = UINT32_MAX;
    for (size_t i = 0; i < n; ++i) {
        *max_tf = std::max(*max_tf, _tfs[base + i]);
        *min_doclen = std::min(*min_doclen, _doclens[base + i]);
    }

    _block_buf.clear();
    _block_buf.push_back(static_cast<char>(n)); // doc_count, n <= kBlockSize (128) fits in a byte
    put_fixed32_le(&_block_buf, _docids[base]); // first_docid (absolute)

    // docid gaps (n-1 values) then tfs (n values), each PFOR-encoded and self-delimiting.
    std::vector<uint32_t> gaps(n > 0 ? n - 1 : 0);
    for (size_t i = 1; i < n; ++i) {
        gaps[i - 1] = _docids[base + i] - _docids[base + i - 1];
    }
    gin_pfor::encode(gaps.data(), gaps.size(), &_block_buf);

    // tfs are already contiguous in _tfs; encode the [base, base+n) slice in place (no copy).
    gin_pfor::encode(_tfs.data() + base, n, &_block_buf);

    Slice s(_block_buf);
    RETURN_IF_ERROR(_block_col->add(&s));
    _next_block_id++;
    return Status::OK();
}

Status BlockPostingWriter::finish_term() {
    const uint32_t num_blocks = (_docids.size() + kBlockSize - 1) / kBlockSize;
    const uint32_t first_block_id = _next_block_id;

    _dir_buf.clear();
    put_fixed32_le(&_dir_buf, num_blocks);
    put_fixed32_le(&_dir_buf, first_block_id);
    for (size_t base = 0; base < _docids.size(); base += kBlockSize) {
        const size_t n = std::min(static_cast<size_t>(kBlockSize), _docids.size() - base);
        uint32_t last_docid, max_tf, min_doclen;
        RETURN_IF_ERROR(_flush_block(base, n, &last_docid, &max_tf, &min_doclen));
        put_fixed32_le(&_dir_buf, last_docid);
        put_fixed32_le(&_dir_buf, max_tf);
        put_fixed32_le(&_dir_buf, min_doclen);
    }

    Slice s(_dir_buf);
    RETURN_IF_ERROR(_dir_col->add(&s));
    return Status::OK();
}

Status BlockPostingWriter::finish(IndexedColumnMetaPB* block_meta, IndexedColumnMetaPB* dir_meta) {
    RETURN_IF_ERROR(_block_col->finish(block_meta));
    RETURN_IF_ERROR(_dir_col->finish(dir_meta));
    return Status::OK();
}

} // namespace starrocks
