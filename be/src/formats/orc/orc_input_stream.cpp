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

#include "formats/orc/orc_input_stream.h"

#include "exprs/cast_expr.h"
#include "formats/orc/orc_mapping.h"
#include "fs/fs.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

ORCHdfsFileStream::ORCHdfsFileStream(RandomAccessFile* file, uint64_t length, io::SharedBufferedInputStream* sb_stream)
        : _file(file), _length(length), _sb_stream(sb_stream) {}

void ORCHdfsFileStream::read(void* buf, uint64_t length, uint64_t offset) {
    if (buf == nullptr) {
        throw orc::ParseError("Buffer is null");
    }
    Status status = _file->read_at_fully(offset, buf, length);
    if (!status.ok()) {
        auto msg = strings::Substitute("Failed to read $0: $1", _file->filename(), status.to_string());
        throw orc::ParseError(msg);
    }
}

const std::string& ORCHdfsFileStream::getName() const {
    return _file->filename();
}

void ORCHdfsFileStream::releaseToOffset(const int64_t offset) {
    if (!_sb_stream) return;
    _sb_stream->release_to_offset(offset);
}

Status ORCHdfsFileStream::setIORanges(const std::vector<io::SharedBufferedInputStream::IORange>& io_ranges,
                                      const bool coalesce_active_lazy_column) {
    if (!_sb_stream) {
        return Status::OK();
    }
    return _sb_stream->set_io_ranges(io_ranges, coalesce_active_lazy_column);
}

bool ORCHdfsFileStream::isAlreadyCollectedInSharedBuffer(const int64_t offset, const int64_t length) const {
    if (!_sb_stream) {
        return false;
    }

    return _sb_stream->find_shared_buffer(offset, length).status().ok();
}

void ORCHdfsFileStream::setIORanges(std::vector<IORange>& io_ranges) {
    if (!_sb_stream) return;

    std::vector<io::SharedBufferedInputStream::IORange> bs_io_ranges;
    bs_io_ranges.reserve(io_ranges.size());
    for (const auto& r : io_ranges) {
        bs_io_ranges.emplace_back(static_cast<int64_t>(r.offset), static_cast<int64_t>(r.size), r.is_active);
    }

    // default we will coalesce active and lazy column into one io range
    bool active_lazy_column_coalesce = true;
    if (isIOAdaptiveCoalesceEnabled() && _lazy_column_coalesce_counter->load(std::memory_order_relaxed) < 0) {
        active_lazy_column_coalesce = false;
        _app_stats->orc_stripe_active_lazy_coalesce_seperately++;
    } else {
        _app_stats->orc_stripe_active_lazy_coalesce_together++;
    }

    const Status st = setIORanges(bs_io_ranges, active_lazy_column_coalesce);

    if (!st.ok()) {
        auto msg = strings::Substitute("Failed to setIORanges $0: $1", _file->filename(), st.to_string());
        throw orc::ParseError(msg);
    }
}

std::atomic<int32_t>* ORCHdfsFileStream::get_lazy_column_coalesce_counter() {
    return _lazy_column_coalesce_counter;
}

} // namespace starrocks
