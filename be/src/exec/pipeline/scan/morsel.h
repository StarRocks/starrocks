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

#include <optional>

#include "exec/query_cache/ticket_checker.h"
#include "gen_cpp/InternalService_types.h"
#include "runtime/mem_pool.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/segment_group.h"
#include "storage/seek_range.h"
#include "storage/tablet.h"
#include "storage/tablet_reader_params.h"
#include "storage/tuple.h"

namespace starrocks {

struct OlapScanRange;
class Tablet;
using TabletSharedPtr = std::shared_ptr<Tablet>;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class Segment;
using SegmentSharedPtr = std::shared_ptr<Segment>;

struct TabletReaderParams;
class SeekTuple;
struct RowidRangeOption;
using RowidRangeOptionPtr = std::shared_ptr<RowidRangeOption>;
struct ShortKeyRangeOption;
using ShortKeyRangeOptionPtr = std::shared_ptr<ShortKeyRangeOption>;
struct ShortKeyOption;
using ShortKeyOptionPtr = std::unique_ptr<ShortKeyOption>;
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace pipeline {

class Morsel;
using MorselPtr = std::unique_ptr<Morsel>;
using Morsels = std::vector<MorselPtr>;
class MorselQueue;
using MorselQueuePtr = std::unique_ptr<MorselQueue>;
using MorselQueueMap = std::unordered_map<int32_t, MorselQueuePtr>;
class MorselQueueFactory;
using MorselQueueFactoryPtr = std::unique_ptr<MorselQueueFactory>;
using MorselQueueFactoryMap = std::unordered_map<int32_t, MorselQueueFactoryPtr>;

/// Morsel.
class Morsel {
public:
    explicit Morsel(int32_t plan_node_id) : _plan_node_id(plan_node_id) {}
    virtual ~Morsel() = default;

    int32_t get_plan_node_id() const { return _plan_node_id; }

    virtual void init_tablet_reader_params(TabletReaderParams* params) {}

    virtual std::tuple<int64_t, int64_t> get_lane_owner_and_version() const {
        return std::tuple<int64_t, int64_t>{0L, 0L};
    }

    // from_version is used when reading incremental rowsets. in default, from_version = 0 means all of the rowsets
    // will be read out. In multi-version cache mechanism, when probing the cache and finding that cached result has
    // stale version, then incremental rowsets in the version range from the cached version till required version
    // should be read out and merged with the cache result, here from_version is cached version.
    void set_from_version(int64_t from_version) { _from_version = from_version; }
    int64_t from_version() { return _from_version; }

    void set_rowsets(const std::vector<RowsetSharedPtr>& rowsets) { _rowsets = &rowsets; }
    void set_delta_rowsets(std::vector<RowsetSharedPtr>&& delta_rowsets) { _delta_rowsets = std::move(delta_rowsets); }
    const std::vector<RowsetSharedPtr>& rowsets() const {
        if (_delta_rowsets.has_value()) {
            return _delta_rowsets.value();
        } else {
            return *_rowsets;
        }
    }

    virtual const std::unordered_set<std::string>& skip_min_max_metrics() const {
        static const std::unordered_set<std::string> metrics;
        return metrics;
    }

private:
    int32_t _plan_node_id;
    int64_t _from_version = 0;

    static const std::vector<RowsetSharedPtr> kEmptyRowsets;
    // _rowsets is owned by MorselQueue, whose lifecycle is longer than that of Morsel.
    const std::vector<RowsetSharedPtr>* _rowsets = &kEmptyRowsets;
    std::optional<std::vector<RowsetSharedPtr>> _delta_rowsets;
};

class ScanMorsel : public Morsel {
public:
    ScanMorsel(int32_t plan_node_id, const TScanRange& scan_range)
            : Morsel(plan_node_id), _scan_range(std::make_unique<TScanRange>(scan_range)) {
        if (_scan_range->__isset.internal_scan_range) {
            _owner_id = _scan_range->internal_scan_range.tablet_id;
            auto str_version = _scan_range->internal_scan_range.version;
            _version = strtol(str_version.c_str(), nullptr, 10);
            _owner_id = _scan_range->internal_scan_range.__isset.bucket_sequence
                                ? _scan_range->internal_scan_range.bucket_sequence
                                : _owner_id;
            _partition_id = _scan_range->internal_scan_range.partition_id;
        }
        if (_scan_range->__isset.binlog_scan_range) {
            _owner_id = _scan_range->binlog_scan_range.tablet_id;
        }
    }

    ~ScanMorsel() override = default;

    ScanMorsel(int32_t plan_node_id, const TScanRangeParams& scan_range)
            : ScanMorsel(plan_node_id, scan_range.scan_range) {}

    TScanRange* get_scan_range() { return _scan_range.get(); }

    TInternalScanRange* get_olap_scan_range() { return &(_scan_range->internal_scan_range); }

    std::tuple<int64_t, int64_t> get_lane_owner_and_version() const override {
        return std::tuple<int64_t, int64_t>{_owner_id, _version};
    }

    int32_t owner_id() const { return _owner_id; }
    int32_t partition_id() const { return _partition_id; }

private:
    std::unique_ptr<TScanRange> _scan_range;
    int64_t _owner_id = 0;
    int64_t _version = 0;
    int64_t _partition_id = 0;
};

/// MorselQueueFactory.
class MorselQueueFactory {
public:
    virtual ~MorselQueueFactory() = default;

    virtual MorselQueue* create(int driver_sequence) = 0;
    virtual size_t size() const = 0;
    virtual size_t num_original_morsels() const = 0;

    virtual bool is_shared() const = 0;
    virtual bool could_local_shuffle() const = 0;
};

class SharedMorselQueueFactory final : public MorselQueueFactory {
public:
    SharedMorselQueueFactory(MorselQueuePtr queue, int size) : _queue(std::move(queue)), _size(size) {}
    ~SharedMorselQueueFactory() override = default;

    MorselQueue* create(int driver_sequence) override { return _queue.get(); }
    size_t size() const override { return _size; }
    size_t num_original_morsels() const override;

    bool is_shared() const override { return true; }
    bool could_local_shuffle() const override { return true; }

private:
    MorselQueuePtr _queue;
    const int _size;
};

class IndividualMorselQueueFactory final : public MorselQueueFactory {
public:
    IndividualMorselQueueFactory(std::map<int, MorselQueuePtr>&& queue_per_driver_seq, bool could_local_shuffle);
    ~IndividualMorselQueueFactory() override = default;

    MorselQueue* create(int driver_sequence) override {
        DCHECK_LT(driver_sequence, _queue_per_driver_seq.size());
        return _queue_per_driver_seq[driver_sequence].get();
    }

    size_t size() const override { return _queue_per_driver_seq.size(); }

    size_t num_original_morsels() const override;

    bool is_shared() const override { return false; }
    bool could_local_shuffle() const override { return _could_local_shuffle; }

private:
    std::vector<MorselQueuePtr> _queue_per_driver_seq;
    const bool _could_local_shuffle;
};

class BucketSequenceMorselQueueFactory final : public MorselQueueFactory {
public:
    BucketSequenceMorselQueueFactory(std::map<int, MorselQueuePtr>&& queue_per_driver_seq, bool could_local_shuffle);
    ~BucketSequenceMorselQueueFactory() override = default;

    MorselQueue* create(int driver_sequence) override {
        DCHECK_LT(driver_sequence, _queue_per_driver_seq.size());
        return _queue_per_driver_seq[driver_sequence].get();
    }

    size_t size() const override { return _queue_per_driver_seq.size(); }

    size_t num_original_morsels() const override;

    bool is_shared() const override { return false; }

    bool could_local_shuffle() const override { return _could_local_shuffle; }

private:
    std::vector<MorselQueuePtr> _queue_per_driver_seq;
    const bool _could_local_shuffle;
};

/// MorselQueue.
class MorselQueue {
public:
    MorselQueue() = default;
    virtual ~MorselQueue() = default;

    virtual std::vector<TInternalScanRange*> olap_scan_ranges() const = 0;

    virtual void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) {}
    virtual void set_tablets(const std::vector<TabletSharedPtr>& tablets) {}
    virtual void set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) {}
    virtual void set_ticket_checker(const query_cache::TicketCheckerPtr& ticket_checker) {}
    virtual bool could_attch_ticket_checker() { return false; }

    virtual size_t num_original_morsels() const = 0;
    virtual size_t max_degree_of_parallelism() const = 0;
    virtual bool empty() const = 0;
    virtual StatusOr<MorselPtr> try_get() = 0;
    void unget(MorselPtr&& morsel);
    virtual std::string name() const = 0;
    virtual StatusOr<bool> ready_for_next() const { return true; }

protected:
    MorselPtr _unget_morsel = nullptr;
};

// The morsel queue with a fixed number of morsels, which is determined in the constructor.
class FixedMorselQueue final : public MorselQueue {
public:
    explicit FixedMorselQueue(Morsels&& morsels)
            : _morsels(std::move(morsels)), _num_morsels(_morsels.size()), _pop_index(0) {}
    ~FixedMorselQueue() override = default;

    std::vector<TInternalScanRange*> olap_scan_ranges() const override;

    void set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) override {
        _tablet_rowsets = tablet_rowsets;
    }

    size_t num_original_morsels() const override { return _num_morsels; }
    size_t max_degree_of_parallelism() const override { return _num_morsels; }
    bool empty() const override { return _unget_morsel == nullptr && _pop_index >= _num_morsels; }
    StatusOr<MorselPtr> try_get() override;

    std::string name() const override { return "fixed_morsel_queue"; }

private:
    Morsels _morsels;
    const size_t _num_morsels;
    std::atomic<size_t> _pop_index;
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;
};

class BucketSequenceMorselQueue : public MorselQueue {
public:
    BucketSequenceMorselQueue(MorselQueuePtr&& morsel_queue);
    std::vector<TInternalScanRange*> olap_scan_ranges() const override;

    void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) override {
        _morsel_queue->set_key_ranges(key_ranges);
    }

    void set_tablets(const std::vector<TabletSharedPtr>& tablets) override { _morsel_queue->set_tablets(tablets); }

    void set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) override {
        _morsel_queue->set_tablet_rowsets(tablet_rowsets);
    }

    void set_ticket_checker(const query_cache::TicketCheckerPtr& ticket_checker) override {
        _ticket_checker = ticket_checker;
    }
    bool could_attch_ticket_checker() override { return true; }

    size_t num_original_morsels() const override { return _morsel_queue->num_original_morsels(); }
    size_t max_degree_of_parallelism() const override { return _morsel_queue->max_degree_of_parallelism(); }
    bool empty() const override;
    StatusOr<MorselPtr> try_get() override;
    std::string name() const override;
    StatusOr<bool> ready_for_next() const override;

private:
    StatusOr<int64_t> _peek_sequence_id() const;

    int64_t _current_sequence = -1;
    MorselQueuePtr _morsel_queue;
    query_cache::TicketCheckerPtr _ticket_checker;
};

class SplitMorselQueue : public MorselQueue {
public:
    SplitMorselQueue(Morsels&& morsels, int64_t degree_of_parallelism, int64_t splitted_scan_rows)
            : _morsels(std::move(morsels)),
              _num_original_morsels(_morsels.size()),
              _degree_of_parallelism(degree_of_parallelism),
              _splitted_scan_rows(splitted_scan_rows) {}
    void set_tablets(const std::vector<TabletSharedPtr>& tablets) override { _tablets = tablets; }

    void set_ticket_checker(const query_cache::TicketCheckerPtr& ticket_checker) override {
        _ticket_checker = ticket_checker;
    }
    bool could_attch_ticket_checker() override { return true; }

protected:
    void _inc_num_splits(bool is_last) {
        if (_ticket_checker == nullptr) {
            return;
        }
        DCHECK(0 <= _tablet_idx && _tablet_idx < _tablets.size());
        auto tablet_id = _tablets[_tablet_idx]->tablet_id();
        _ticket_checker->enter(tablet_id, is_last);
    }

    ScanMorsel* _cur_scan_morsel() { return down_cast<ScanMorsel*>(_morsels[_tablet_idx].get()); }

    const Morsels _morsels;
    // The number of the morsels before split them to pieces.
    const size_t _num_original_morsels;
    const int64_t _degree_of_parallelism;
    // The minimum number of rows picked up from a segment at one time.
    const int64_t _splitted_scan_rows;

    std::vector<TabletSharedPtr> _tablets;
    std::atomic<size_t> _tablet_idx = 0;
    query_cache::TicketCheckerPtr _ticket_checker;
};

class PhysicalSplitMorselQueue final : public SplitMorselQueue {
public:
    using SplitMorselQueue::SplitMorselQueue;
    ~PhysicalSplitMorselQueue() override = default;

    std::vector<TInternalScanRange*> olap_scan_ranges() const override;

    void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) override;
    void set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) override {
        _tablet_rowsets = tablet_rowsets;
    }

    size_t num_original_morsels() const override { return _morsels.size(); }
    size_t max_degree_of_parallelism() const override { return _degree_of_parallelism; }
    bool empty() const override { return _unget_morsel == nullptr && _tablet_idx >= _tablets.size(); }
    StatusOr<MorselPtr> try_get() override;

    std::string name() const override { return "physical_split_morsel_queue"; }

private:
    rowid_t _lower_bound_ordinal(Segment* segment, const SeekTuple& key, bool lower) const;
    rowid_t _upper_bound_ordinal(Segment* segment, const SeekTuple& key, bool lower, rowid_t end) const;
    bool _is_last_split_of_current_morsel();

    Rowset* _cur_rowset();
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

    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;

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

    std::vector<TInternalScanRange*> olap_scan_ranges() const override;

    void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) override;
    void set_tablets(const std::vector<TabletSharedPtr>& tablets) override { _tablets = tablets; }
    void set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) override {
        _tablet_rowsets = tablet_rowsets;
    }

    size_t num_original_morsels() const override { return _morsels.size(); }
    size_t max_degree_of_parallelism() const override { return _degree_of_parallelism; }
    bool empty() const override { return _unget_morsel == nullptr && _tablet_idx >= _tablets.size(); }
    StatusOr<MorselPtr> try_get() override;

    std::string name() const override { return "logical_split_morsel_queue"; }

private:
    bool _cur_tablet_finished() const;

    Rowset* _find_largest_rowset(const std::vector<RowsetSharedPtr>& rowsets);
    SegmentSharedPtr _find_largest_segment(Rowset* rowset) const;
    StatusOr<SegmentGroupPtr> _create_segment_group(Rowset* rowset);
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

    // _tablets[i] and _tablet_rowsets[i] represent the i-th tablet and its rowsets.
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;

    bool _has_init_any_tablet = false;
    bool _is_first_split_of_tablet = true;

    // Used to allocate memory for _tablet_seek_ranges.
    MemPool _mempool;
    std::vector<SeekRange> _tablet_seek_ranges;
    Rowset* _largest_rowset = nullptr;
    SegmentGroupPtr _segment_group = nullptr;
    SchemaPtr _short_key_schema = nullptr;
    int64_t _sample_splitted_scan_blocks = 0;

    std::vector<std::pair<ShortKeyIndexGroupIterator, ShortKeyIndexGroupIterator>> _block_ranges_per_seek_range;
    std::vector<size_t> _num_rest_blocks_per_seek_range;
    size_t _range_idx = 0;
    ShortKeyIndexGroupIterator _next_lower_block_iter;
};

MorselQueuePtr create_empty_morsel_queue();

} // namespace pipeline
} // namespace starrocks