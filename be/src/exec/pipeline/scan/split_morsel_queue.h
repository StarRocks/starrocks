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

#include <mutex>

#include "exec/pipeline/scan/olap_morsel_queue.h"
#include "exec/pipeline/scan/ticketed_morsel_queue.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "storage/primitive/range.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/segment_group.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/seek_range.h"
#include "storage/tablet.h"

namespace starrocks::pipeline {

class SplitMorselQueue : public OlapMorselQueue, public TicketedMorselQueue {
public:
    SplitMorselQueue(Morsels&& morsels, int64_t degree_of_parallelism, int64_t splitted_scan_rows)
            : OlapMorselQueue(std::move(morsels)),
              _degree_of_parallelism(degree_of_parallelism),
              _splitted_scan_rows(splitted_scan_rows) {}
    void set_ticket_checker(const query_cache::TicketCheckerPtr& ticket_checker) override {
        _ticket_checker = ticket_checker;
    }
    bool could_attch_ticket_checker() const override { return true; }
    size_t max_degree_of_parallelism() const override { return _degree_of_parallelism; }
    Type type() const override { return SPLIT; }

protected:
    void _inc_split(bool is_last_split) {
        if (_ticket_checker == nullptr) {
            return;
        }
        DCHECK(0 <= _tablet_idx && _tablet_idx < _tablets.size());
        auto tablet_id = _tablets[_tablet_idx]->tablet_id();
        _ticket_checker->enter(tablet_id, is_last_split);
    }
    ScanMorsel* _cur_scan_morsel() { return down_cast<ScanMorsel*>(_morsels[_tablet_idx].get()); }

    // The number of the morsels before split them to pieces.
    const int64_t _degree_of_parallelism;
    // The minimum number of rows picked up from a segment at one time.
    const int64_t _splitted_scan_rows;

    std::atomic<size_t> _tablet_idx = 0;
    query_cache::TicketCheckerPtr _ticket_checker;
};

class PhysicalSplitMorselQueue final : public SplitMorselQueue {
public:
    using SplitMorselQueue::SplitMorselQueue;
    ~PhysicalSplitMorselQueue() override = default;

    void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) override;
    void set_key_ranges(const TabletReaderParams::RangeStartOperation& range_start_op,
                        const TabletReaderParams::RangeEndOperation& range_end_op,
                        const std::vector<OlapTuple>& range_start_key,
                        const std::vector<OlapTuple>& range_end_key) override;
    bool empty() const override { return _unget_morsel == nullptr && _tablet_idx >= _tablets.size(); }
    StatusOr<MorselPtr> try_get() override;

    std::string name() const override { return "physical_split_morsel_queue"; }
    Type type() const override { return PHYSICAL_SPLIT; }

private:
    rowid_t _lower_bound_ordinal(Segment* segment, const SeekTuple& key, bool lower) const;
    rowid_t _upper_bound_ordinal(Segment* segment, const SeekTuple& key, bool lower, rowid_t end) const;
    bool _is_last_split_of_current_morsel();

    BaseRowset* _cur_rowset();
    // Return nullptr, when _segment_idx exceeds the segments of the current rowset.
    Segment* _cur_segment();

    // Returning false means that there is no more segment to read.
    bool _next_segment();
    // Load the meta of the new rowset and the index of the new segment,
    // and find the rowid range of each key range in this segment.
    Status _init_segment();
    // Obtain row id ranges from multiple segments of multiple rowsets within a single tablet,
    // until _splitted_scan_rows rows are retrieved.
    StatusOr<RowidRangeOptionPtr> _try_get_split_from_single_tablet();

private:
    std::mutex _mutex;

    /// Key ranges passed to the storage layer.
    TabletReaderParams::RangeStartOperation _range_start_op = TabletReaderParams::RangeStartOperation::GT;
    TabletReaderParams::RangeEndOperation _range_end_op = TabletReaderParams::RangeEndOperation::LT;
    std::vector<OlapTuple> _range_start_key;
    std::vector<OlapTuple> _range_end_key;

    // _tablets[i] and _tablet_rowsets[i] represent the i-th tablet and its rowsets.
    bool _has_init_any_segment = false;
    bool _is_first_split_of_segment = true;

    size_t _rowset_idx = 0;
    size_t _segment_idx = 0;
    std::vector<SeekRange> _tablet_seek_ranges;
    SparseRange<> _segment_scan_range;
    SparseRangeIterator<> _segment_range_iter;
    // The number of unprocessed rows of the current segment.
    size_t _num_segment_rest_rows = 0;

    MemPool _mempool;
};

class LogicalSplitMorselQueue final : public SplitMorselQueue {
public:
    using SplitMorselQueue::SplitMorselQueue;
    ~LogicalSplitMorselQueue() override = default;

    void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) override;
    void set_key_ranges(const TabletReaderParams::RangeStartOperation& range_start_op,
                        const TabletReaderParams::RangeEndOperation& range_end_op,
                        const std::vector<OlapTuple>& range_start_key,
                        const std::vector<OlapTuple>& range_end_key) override;
    bool empty() const override { return _unget_morsel == nullptr && _tablet_idx >= _tablets.size(); }
    StatusOr<MorselPtr> try_get() override;

    std::string name() const override { return "logical_split_morsel_queue"; }
    Type type() const override { return LOGICAL_SPLIT; }

private:
    bool _cur_tablet_finished() const;

    BaseRowset* _find_largest_rowset(const std::vector<BaseRowsetSharedPtr>& rowsets);
    SegmentSharedPtr _find_largest_segment(BaseRowset* rowset) const;
    StatusOr<SegmentGroupPtr> _create_segment_group(BaseRowset* rowset);
    bool _next_tablet();
    Status _init_tablet();

    ShortKeyOptionPtr _create_range_lower() const;
    ShortKeyOptionPtr _create_range_upper() const;
    bool _valid_range(const ShortKeyOptionPtr& lower, const ShortKeyOptionPtr& upper) const;

    ShortKeyIndexGroupIterator _lower_bound_ordinal(const SeekTuple& key, bool lower) const;
    ShortKeyIndexGroupIterator _upper_bound_ordinal(const SeekTuple& key, bool lower) const;
    bool _is_last_split_of_current_morsel();

private:
    std::mutex _mutex;

    /// Key ranges passed to the storage layer.
    TabletReaderParams::RangeStartOperation _range_start_op = TabletReaderParams::RangeStartOperation::GT;
    TabletReaderParams::RangeEndOperation _range_end_op = TabletReaderParams::RangeEndOperation::LT;
    std::vector<OlapTuple> _range_start_key;
    std::vector<OlapTuple> _range_end_key;

    bool _has_init_any_tablet = false;
    bool _is_first_split_of_tablet = true;

    // Used to allocate memory for _tablet_seek_ranges.
    MemPool _mempool;
    std::vector<SeekRange> _tablet_seek_ranges;
    BaseRowset* _largest_rowset = nullptr;
    SegmentGroupPtr _segment_group = nullptr;
    SchemaPtr _short_key_schema = nullptr;
    int64_t _sample_splitted_scan_blocks = 0;

    std::vector<std::pair<ShortKeyIndexGroupIterator, ShortKeyIndexGroupIterator>> _block_ranges_per_seek_range;
    std::vector<size_t> _num_rest_blocks_per_seek_range;
    size_t _range_idx = 0;
    ShortKeyIndexGroupIterator _next_lower_block_iter;
};

} // namespace starrocks::pipeline
