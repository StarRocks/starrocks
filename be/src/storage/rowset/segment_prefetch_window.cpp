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

#include "storage/rowset/segment_prefetch_window.h"

#include <algorithm>

#include "common/logging.h"
#include "storage/rowset/column_reader.h"
#include "storage_primitive/storage_stats.h"

namespace starrocks {

// See SegmentPrefetchWindow::warm_rows().
static constexpr int kLateReadAheadMinBatches = 4;

SegmentPrefetchWindow::SegmentPrefetchWindow(std::vector<Column> columns, const SparseRange<>* scan_range,
                                             rowid_t start_row, int64_t object_offset, size_t ahead_blocks,
                                             double late_read_ahead_ratio, size_t block_size,
                                             OlapReaderStatistics* stats)
        : _scan_range(scan_range),
          _object_offset(object_offset),
          _ahead_limit(ahead_blocks),
          _late_read_ahead_ratio(late_read_ahead_ratio),
          _block_size(std::max<int64_t>(1, static_cast<int64_t>(block_size))),
          _stats(stats) {
    add_cursors(columns, start_row);
}

void SegmentPrefetchWindow::add_cursors(const std::vector<Column>& columns, rowid_t start_row) {
    _cursors.reserve(_cursors.size() + columns.size());
    for (const auto& column : columns) {
        Cursor c;
        c.column = column;
        if (_scan_range->empty() || start_row >= _scan_range->end() ||
            !column.reader->seek_at_or_before(std::max(start_row, _scan_range->begin()), &c.page).ok()) {
            c.done = true;
        } else {
            settle(&c);
        }
        _cursors.push_back(c);
    }
}

void SegmentPrefetchWindow::settle(Cursor* c) {
    const SparseRange<>& ranges = *_scan_range;
    while (!c->done) {
        if (!c->page.valid()) {
            c->done = true;
            return;
        }
        const ordinal_t first = c->page.first_ordinal();
        const ordinal_t end = c->page.last_ordinal() + 1;
        while (c->range_idx < ranges.size() && ranges[c->range_idx].end() <= first) {
            c->range_idx++;
        }
        if (c->range_idx >= ranges.size()) {
            c->done = true;
            return;
        }
        const rowid_t next_row = ranges[c->range_idx].begin();
        if (next_row < end) {
            return;
        }
        // The scan range skips the rest of this page: jump to the page holding its next row.
        OrdinalPageIndexIterator it;
        if (!c->column.reader->seek_at_or_before(next_row, &it).ok()) {
            c->done = true;
            return;
        }
        if (it.page_index() > c->page.page_index()) {
            c->page = it;
        } else {
            c->page.next();
        }
    }
}

bool SegmentPrefetchWindow::warm(io::SeekableInputStream* file, int64_t offset, int64_t size, bool urgent,
                                 std::shared_ptr<io::CacheWarmHandle>* handle) {
    auto res = file->warm_cache_async(offset, size, urgent);
    if (!res.ok()) {
        // Nothing here is required for correctness: the reads fetch whatever was not warmed. A stream
        // that cannot warm at all will not start to, so stop asking.
        VLOG(2) << "stop warming " << file->filename() << ": " << res.status();
        _disabled = true;
        _inflight.clear();
        _ahead_blocks = 0;
        _row_batch.clear();
        return false;
    }
    _stats->remote_prefetch_ranges++;
    *handle = std::move(res).value();
    return true;
}

void SegmentPrefetchWindow::submit(size_t cursor_idx, rowid_t row) {
    Cursor& c = _cursors[cursor_idx];
    const PagePointer pp = c.page.page();
    const rowid_t begin_row = static_cast<rowid_t>(c.page.first_ordinal());
    const rowid_t end_row = static_cast<rowid_t>(c.page.last_ordinal() + 1);
    const int64_t begin = _object_offset + static_cast<int64_t>(pp.offset);
    const int64_t end = begin + pp.size;
    const int64_t first_block = begin / _block_size;
    const int64_t last_block = (end - 1) / _block_size;
    c.page.next();
    settle(&c);

    if (end_row <= row || pp.size == 0) {
        // The reader is past this page already.
        return;
    }
    if (last_block <= c.last_block) {
        // Pages smaller than a cache block: an earlier page of this column asked for this block already,
        // and the request now has to stay until the reader is past this page too.
        for (auto it = _inflight.rbegin(); it != _inflight.rend(); ++it) {
            if (it->cursor_idx == cursor_idx) {
                it->end_row = std::max(it->end_row, end_row);
                break;
            }
        }
        return;
    }
    const int64_t from_block = std::max(first_block, c.last_block + 1);
    const int64_t warm_begin = std::max(begin, from_block * _block_size);
    std::shared_ptr<io::CacheWarmHandle> handle;
    if (!warm(c.column.file, warm_begin - _object_offset, end - warm_begin, false /* urgent */, &handle)) {
        return;
    }
    const size_t blocks = static_cast<size_t>(last_block - from_block + 1);
    c.last_block = last_block;
    _inflight.push_back(InFlight{cursor_idx, begin_row, end_row, blocks, std::move(handle)});
    _ahead_blocks += blocks;
}

void SegmentPrefetchWindow::advance(rowid_t row) {
    if (_disabled) {
        return;
    }
    // Forget the pages the reader is done with; a block of them still queued is withdrawn unless some
    // other reader waits for it.
    auto consumed = std::remove_if(_inflight.begin(), _inflight.end(), [&](const InFlight& e) {
        if (e.end_row <= row) {
            _ahead_blocks -= e.blocks;
            return true;
        }
        return false;
    });
    _inflight.erase(consumed, _inflight.end());

    while (_ahead_blocks < _ahead_limit && !_disabled) {
        size_t next = _cursors.size();
        for (size_t i = 0; i < _cursors.size(); ++i) {
            if (!_cursors[i].done &&
                (next == _cursors.size() || _cursors[i].page.first_ordinal() < _cursors[next].page.first_ordinal())) {
                next = i;
            }
        }
        if (next == _cursors.size()) {
            break;
        }
        submit(next, row);
    }
}

void SegmentPrefetchWindow::warm_rows(const std::vector<Column>& columns, const rowid_t* rowids, size_t n) {
    _row_batch.clear();
    if (_disabled || n == 0) {
        return;
    }
    for (const auto& column : columns) {
        // Contiguous pages holding the rows become one warm-up range.
        int64_t run_begin = -1;
        int64_t run_end = -1;
        int32_t first_page = -1;
        int32_t last_page = -1;
        size_t i = 0;
        while (i < n) {
            OrdinalPageIndexIterator it;
            if (!column.reader->seek_at_or_before(rowids[i], &it).ok()) {
                break;
            }
            if (first_page < 0) {
                first_page = it.page_index();
            }
            last_page = it.page_index();
            _late_pages_touched++;
            const PagePointer pp = it.page();
            if (run_end == static_cast<int64_t>(pp.offset)) {
                run_end += pp.size;
            } else {
                if (run_begin >= 0) {
                    std::shared_ptr<io::CacheWarmHandle> handle;
                    if (!warm(column.file, run_begin, run_end - run_begin, true /* urgent */, &handle)) {
                        return;
                    }
                    _row_batch.push_back(std::move(handle));
                }
                run_begin = pp.offset;
                run_end = static_cast<int64_t>(pp.offset) + pp.size;
            }
            const ordinal_t page_last = it.last_ordinal();
            i = std::upper_bound(rowids + i, rowids + n, page_last) - rowids;
        }
        if (run_begin >= 0 && run_end > run_begin) {
            std::shared_ptr<io::CacheWarmHandle> handle;
            if (!warm(column.file, run_begin, run_end - run_begin, true /* urgent */, &handle)) {
                return;
            }
            _row_batch.push_back(std::move(handle));
        }
        if (first_page >= 0) {
            _late_pages_spanned += last_page - first_page + 1;
        }
    }
    if (!_late_read_ahead && ++_late_batches >= kLateReadAheadMinBatches && _late_read_ahead_ratio <= 1 &&
        static_cast<double>(_late_pages_touched) >= _late_read_ahead_ratio * static_cast<double>(_late_pages_spanned)) {
        _late_read_ahead = true;
        _stats->remote_prefetch_late_read_ahead++;
        add_cursors(columns, rowids[n - 1]);
    }
}

} // namespace starrocks
