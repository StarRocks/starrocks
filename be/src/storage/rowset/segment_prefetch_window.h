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

#include <algorithm>
#include <memory>
#include <vector>

#include "io/seekable_input_stream.h"
#include "storage/rowset/ordinal_page_index.h"
#include "storage/rowset/read_ahead_budget.h"
#include "storage_primitive/range.h"
#include "storage_primitive/rowid_types.h"

namespace starrocks {

class ColumnReader;
struct OlapReaderStatistics;

// Keeps the cache blocks a segment reader is about to consume warm, when the remote I/O scheduler is
// enabled: on a cold cache the blocks of the next pages are fetched in parallel instead of one round trip
// after another as the reader reaches them.
//
// The window walks, in row order, the data pages of the given columns that intersect the final scan
// range, and asks for their cache blocks until the reader has `ahead_blocks` of them ahead of what it
// consumes -- queued, in flight, or warmed and not read yet. That bound only keeps the reader from running
// so far ahead that it evicts what it still needs; when the blocks are fetched, and for whom first, is the
// scheduler's decision. It never guesses: only pages the reader will read (per the ordinal index and the
// scan range) are warmed, and a page leaves the window once the reader has passed its last row. The page
// list is produced incrementally, so the window's memory does not grow with the segment. A page the reader will take
// from the memory page cache is not warmed: its read does not touch the file. A window given `probe_columns` asks for
// nothing more once the reader's first pages turn out cached (see idle()).
//
// The blocks of the query windows of a node are bounded by ReadAheadBudget (lake_scan_read_ahead_max_blocks): a window
// asks for a grant before it warms blocks, and a window that holds its share, or finds the node full, asks for no
// more until it has given some back. Each warm-up keeps its grant until the reader has passed its pages: the
// blocks count until they are read, whether queued, in flight or fetched. Beyond that the reads fetch what they reach.
class SegmentPrefetchWindow {
public:
    struct Column {
        ColumnReader* reader = nullptr;
        io::SeekableInputStream* file = nullptr;
        // Where the column's file starts in its object (non-zero for a bundle file), so that blocks are
        // counted where the cache stores them.
        int64_t object_offset = 0;
        // Whether the reads of this column look their pages up in the memory page cache first, under the keys
        // `file` gives them.
        bool use_page_cache = false;
    };

    // `scan_range` (ascending) must outlive the window; the pages before `start_row` are skipped. `budget` bounds the
    // blocks of all windows that share it; nullptr leaves the window to its own `ahead_blocks` only.
    // `probe_columns`: the columns the reader reads ahead and those it reads late (see warm_rows()), whose first pages
    // tell whether the segment is cached (see idle()); empty for a window that does not go idle.
    SegmentPrefetchWindow(std::vector<Column> columns, const SparseRange<>* scan_range, rowid_t start_row,
                          size_t ahead_blocks, double late_read_ahead_ratio, size_t block_size,
                          OlapReaderStatistics* stats, ReadAheadBudget* budget, std::vector<Column> probe_columns = {});

    // The reader is about to read the rows from `row` on: forget what it has consumed and top the
    // window up again.
    void advance(rowid_t row);

    // Warms the pages of `columns` that hold the `n` ascending `rowids`, for a read about to happen --
    // the late-materialized columns of one chunk -- as a demand: the read needs them next, so the
    // read-ahead bound does not hold them back. They replace the previous such batch.
    //
    // It also measures how much of these columns the reads need. Once the rows of kLateReadAheadMinBatches
    // batches have touched at least `late_read_ahead_ratio` of the pages from the first one they touched to the
    // last -- the gaps between batches, batches with no row left included -- `columns` are read ahead
    // like the others from then on: nearly every page is needed anyway, and fetching only what each batch
    // turned out to need keeps few GETs in flight while the reader waits for every batch.
    void warm_rows(const std::vector<Column>& columns, const rowid_t* rowids, size_t n);
    bool reads_ahead_late_columns() const { return _late_read_ahead; }

    bool disabled() const { return _disabled; }
    // Set when, at the first advance(), the page holding the reader's next row needed no fetch in any of the
    // `probe_columns`. The segment was most likely read as a whole recently, and looking each of its pages up would
    // cost more than the reads it could speed up, so the window asks for nothing more. One column's page says little:
    // a page that covers the rows of many readers (a key or low-cardinality column) is cached as soon as any of them
    // reads it, and the reader's own short key lookup reads the key columns' pages. A page whose read-ahead the
    // scheduler refused, its queue being full, counts as needing no fetch too: no more read-ahead would get in either.
    bool idle() const { return _idle; }

    // Cache blocks asked for and not consumed yet.
    size_t ahead_blocks() const { return _ahead_blocks; }
    // Whether every page the reader will read of the read-ahead columns has been asked for.
    bool exhausted() const {
        return _disabled || _idle ||
               std::all_of(_cursors.begin(), _cursors.end(), [](const Cursor& c) { return c.done; });
    }

private:
    struct Cursor {
        Column column;
        OrdinalPageIndexIterator page;
        // First scan range that does not end before the current page.
        size_t range_idx = 0;
        bool done = false;
        // Last cache block this window has asked for in this column; -1 before the first.
        int64_t last_block = -1;
        // Row after which the reader no longer needs the blocks asked for last.
        rowid_t last_end_row = 0;
    };
    struct InFlight {
        size_t cursor_idx;
        // The rows of the pages the blocks hold.
        rowid_t begin_row;
        rowid_t end_row;
        size_t blocks;
        std::shared_ptr<io::CacheWarmHandle> handle;
        // The blocks it holds of the node's budget.
        ReadAheadBudget::Grant grant;
    };

    // Moves `c` to the first page at or after its current one that intersects the scan range.
    void settle(Cursor* c);
    // Reads `columns` ahead from `start_row` on.
    void add_cursors(const std::vector<Column>& columns, rowid_t start_row);
    // Asks for the next blocks of `cursor_idx`. False when the budget grants none or the window stopped (disabled or
    // idle).
    bool submit(size_t cursor_idx, rowid_t row);
    // The blocks the window holds of the node's budget.
    size_t granted_blocks() const;
    // Stops reading ahead: drops every warm-up and leaves the budget.
    void disable();
    // Looks at the page holding `row` in each of the `probe_columns`, and goes idle if none needs a fetch (see idle()).
    // The warm-ups it asked for go to `handles`.
    void probe(rowid_t row, std::vector<std::shared_ptr<io::CacheWarmHandle>>* handles);
    // Goes idle (see idle()): drops every warm-up and leaves the budget.
    void set_idle();
    // Whether the read of `page` will find it in the memory page cache instead of reading the file.
    static bool in_page_cache(const Column& column, const PagePointer& page);
    // `urgent`: pages a read is about to consume, fetched in parallel as a demand rather than as read-ahead.
    bool warm(io::SeekableInputStream* file, int64_t offset, int64_t size, bool urgent,
              std::shared_ptr<io::CacheWarmHandle>* handle);

    std::vector<Cursor> _cursors;
    const SparseRange<>* _scan_range;
    const size_t _ahead_limit;
    const double _late_read_ahead_ratio;
    const int64_t _block_size;
    OlapReaderStatistics* _stats;
    // Warmed pages the reader has not passed yet, and their blocks.
    std::vector<InFlight> _inflight;
    size_t _ahead_blocks = 0;
    ReadAheadBudget* const _budget;
    ReadAheadBudget::Member _member;
    std::vector<std::shared_ptr<io::CacheWarmHandle>> _row_batch;
    // Set when the stream cannot warm the cache at all (e.g. a non-starlet file); the window then
    // stays out of the way.
    bool _disabled = false;
    // Left until the first advance() has probed them (see idle()).
    std::vector<Column> _probe_columns;
    bool _idle = false;
    // Distinct pages of the late columns the batches of warm_rows() touched, the first and the last page each column touched
    // (-1 before its first), the number of batches, and whether those columns are read ahead now.
    int64_t _late_pages_touched = 0;
    std::vector<int32_t> _late_first_page;
    std::vector<int32_t> _late_last_page;
    int _late_batches = 0;
    bool _late_read_ahead = false;
};

} // namespace starrocks
