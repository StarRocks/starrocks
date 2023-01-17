// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <optional>

#include "gen_cpp/InternalService_types.h"
#include "runtime/mem_pool.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/segment_group.h"
#include "storage/seek_range.h"
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

namespace vectorized {
class TabletReaderParams;
class SeekTuple;
struct RowidRangeOption;
using RowidRangeOptionPtr = std::shared_ptr<RowidRangeOption>;
struct ShortKeyRangeOption;
using ShortKeyRangeOptionPtr = std::shared_ptr<ShortKeyRangeOption>;
struct ShortKeyOption;
using ShortKeyOptionPtr = std::unique_ptr<vectorized::ShortKeyOption>;
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;
class Range;
} // namespace vectorized

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
    Morsel(int32_t plan_node_id) : _plan_node_id(plan_node_id) {}
    virtual ~Morsel() = default;

    int32_t get_plan_node_id() const { return _plan_node_id; }

    virtual void init_tablet_reader_params(vectorized::TabletReaderParams* params) {}

    void set_rowsets(std::vector<RowsetSharedPtr> rowsets) { _rowsets = std::move(rowsets); }
    const std::vector<RowsetSharedPtr>& rowsets() const { return _rowsets; }

private:
    int32_t _plan_node_id;

    std::vector<RowsetSharedPtr> _rowsets;
};

class ScanMorsel : public Morsel {
public:
    ScanMorsel(int32_t plan_node_id, const TScanRange& scan_range)
            : Morsel(plan_node_id), _scan_range(std::make_unique<TScanRange>(scan_range)) {}

    ScanMorsel(int32_t plan_node_id, const TScanRangeParams& scan_range)
            : ScanMorsel(plan_node_id, scan_range.scan_range) {}

    TScanRange* get_scan_range() { return _scan_range.get(); }

    TInternalScanRange* get_olap_scan_range() { return &(_scan_range->internal_scan_range); }

private:
    std::unique_ptr<TScanRange> _scan_range;
};

class PhysicalSplitScanMorsel final : public ScanMorsel {
public:
    PhysicalSplitScanMorsel(int32_t plan_node_id, const TScanRange& scan_range,
                            vectorized::RowidRangeOptionPtr rowid_range_option)
            : ScanMorsel(plan_node_id, scan_range), _rowid_range_option(std::move(rowid_range_option)) {}

    void init_tablet_reader_params(vectorized::TabletReaderParams* params) override;

private:
    vectorized::RowidRangeOptionPtr _rowid_range_option;
};

class LogicalSplitScanMorsel final : public ScanMorsel {
public:
    LogicalSplitScanMorsel(int32_t plan_node_id, const TScanRange& scan_range,
                           std::vector<vectorized::ShortKeyRangeOptionPtr> short_key_ranges)
            : ScanMorsel(plan_node_id, scan_range), _short_key_ranges(std::move(short_key_ranges)) {}

    void init_tablet_reader_params(vectorized::TabletReaderParams* params) override;

private:
    std::vector<vectorized::ShortKeyRangeOptionPtr> _short_key_ranges;
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

/// MorselQueue.
class MorselQueue {
public:
    MorselQueue() = default;
    virtual ~MorselQueue() = default;

    virtual std::vector<TInternalScanRange*> olap_scan_ranges() const = 0;

    virtual void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) {}
    virtual void set_tablets(const std::vector<TabletSharedPtr>& tablets) {}
    virtual void set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) {}

    virtual size_t num_original_morsels() const = 0;
    virtual size_t max_degree_of_parallelism() const = 0;
    virtual bool empty() const = 0;
    virtual StatusOr<MorselPtr> try_get() = 0;

    virtual std::string name() const = 0;
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
    bool empty() const override { return _pop_index >= _num_morsels; }
    StatusOr<MorselPtr> try_get() override;

    std::string name() const override { return "fixed_morsel_queue"; }

private:
    Morsels _morsels;
    const size_t _num_morsels;
    std::atomic<size_t> _pop_index;
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;
};

class PhysicalSplitMorselQueue final : public MorselQueue {
public:
    explicit PhysicalSplitMorselQueue(Morsels&& morsels, int64_t degree_of_parallelism, int64_t splitted_scan_rows)
            : _morsels(std::move(morsels)),
              _num_original_morsels(_morsels.size()),
              _degree_of_parallelism(degree_of_parallelism),
              _splitted_scan_rows(splitted_scan_rows) {}
    ~PhysicalSplitMorselQueue() override = default;

    std::vector<TInternalScanRange*> olap_scan_ranges() const override;

    void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) override;
    void set_tablets(const std::vector<TabletSharedPtr>& tablets) override { _tablets = tablets; }
    void set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) override {
        _tablet_rowsets = tablet_rowsets;
    }

    size_t num_original_morsels() const override { return _morsels.size(); }
    size_t max_degree_of_parallelism() const override { return _degree_of_parallelism; }
    bool empty() const override { return _tablet_idx >= _tablets.size(); }
    StatusOr<MorselPtr> try_get() override;

    std::string name() const override { return "physical_split_morsel_queue"; }

private:
    rowid_t _lower_bound_ordinal(Segment* segment, const vectorized::SeekTuple& key, bool lower) const;
    rowid_t _upper_bound_ordinal(Segment* segment, const vectorized::SeekTuple& key, bool lower, rowid_t end) const;

    ScanMorsel* _cur_scan_morsel();
    Rowset* _cur_rowset();
    // Return nullptr, when _segment_idx exceeds the segments of the current rowset.
    Segment* _cur_segment();

    // Returning false means that there is no more segment to read.
    bool _next_segment();
    // Load the meta of the new rowset and the index of the new segment,
    // and find the rowid range of each key range in this segment.
    Status _init_segment();

private:
    std::mutex _mutex;

    const Morsels _morsels;
    // The number of the morsels before split them to pieces.
    const size_t _num_original_morsels;
    const int64_t _degree_of_parallelism;
    // The minimum number of rows picked up from a segment at one time.
    const int64_t _splitted_scan_rows;

    /// Key ranges passed to the storage layer.
    vectorized::TabletReaderParams::RangeStartOperation _range_start_op =
            vectorized::TabletReaderParams::RangeStartOperation::GT;
    vectorized::TabletReaderParams::RangeEndOperation _range_end_op =
            vectorized::TabletReaderParams::RangeEndOperation::LT;
    std::vector<OlapTuple> _range_start_key;
    std::vector<OlapTuple> _range_end_key;

    // _tablets[i] and _tablet_rowsets[i] represent the i-th tablet and its rowsets.
    std::vector<TabletSharedPtr> _tablets;
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;

    bool _has_init_any_segment = false;
    std::atomic<size_t> _tablet_idx = 0;
    size_t _rowset_idx = 0;
    size_t _segment_idx = 0;
    std::vector<vectorized::SeekRange> _tablet_seek_ranges;
    vectorized::SparseRange _segment_scan_range;
    vectorized::SparseRangeIterator _segment_range_iter;
    // The number of unprocessed rows of the current segment.
    size_t _num_segment_rest_rows = 0;

    MemPool _mempool;
};

class LogicalSplitMorselQueue final : public MorselQueue {
public:
    explicit LogicalSplitMorselQueue(Morsels&& morsels, int64_t degree_of_parallelism, int64_t splitted_scan_rows)
            : _morsels(std::move(morsels)),
              _num_original_morsels(_morsels.size()),
              _degree_of_parallelism(degree_of_parallelism),
              _splitted_scan_rows(splitted_scan_rows) {}
    ~LogicalSplitMorselQueue() override = default;

    std::vector<TInternalScanRange*> olap_scan_ranges() const override;

    void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) override;
    void set_tablets(const std::vector<TabletSharedPtr>& tablets) override { _tablets = tablets; }
    void set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) override {
        _tablet_rowsets = tablet_rowsets;
    }

    size_t num_original_morsels() const override { return _morsels.size(); }
    size_t max_degree_of_parallelism() const override { return _degree_of_parallelism; }
    bool empty() const override { return _tablet_idx >= _tablets.size(); }
    StatusOr<MorselPtr> try_get() override;

    std::string name() const override { return "logical_split_morsel_queue"; }

private:
    bool _cur_tablet_finished() const;

    Rowset* _find_largest_rowset(const std::vector<RowsetSharedPtr>& rowsets);
    SegmentSharedPtr _find_largest_segment(Rowset* rowset) const;
    StatusOr<SegmentGroupPtr> _create_segment_group(Rowset* rowset);
    bool _next_tablet();
    Status _init_tablet();

    vectorized::ShortKeyOptionPtr _create_range_lower() const;
    vectorized::ShortKeyOptionPtr _create_range_upper() const;
    bool _valid_range(const vectorized::ShortKeyOptionPtr& lower, const vectorized::ShortKeyOptionPtr& upper) const;

    ShortKeyIndexGroupIterator _lower_bound_ordinal(const vectorized::SeekTuple& key, bool lower) const;
    ShortKeyIndexGroupIterator _upper_bound_ordinal(const vectorized::SeekTuple& key, bool lower) const;

private:
    std::mutex _mutex;

    Morsels _morsels;
    // The number of the morsels before split them to pieces.
    const size_t _num_original_morsels;
    const int64_t _degree_of_parallelism;
    // The minimum number of rows picked up from a segment at one time.
    const int64_t _splitted_scan_rows;

    /// Key ranges passed to the storage layer.
    vectorized::TabletReaderParams::RangeStartOperation _range_start_op =
            vectorized::TabletReaderParams::RangeStartOperation::GT;
    vectorized::TabletReaderParams::RangeEndOperation _range_end_op =
            vectorized::TabletReaderParams::RangeEndOperation::LT;
    std::vector<OlapTuple> _range_start_key;
    std::vector<OlapTuple> _range_end_key;

    // _tablets[i] and _tablet_rowsets[i] represent the i-th tablet and its rowsets.
    std::vector<TabletSharedPtr> _tablets;
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;

    bool _has_init_any_tablet = false;
    std::atomic<size_t> _tablet_idx = 0;

    // Used to allocate memory for _tablet_seek_ranges.
    MemPool _mempool;
    std::vector<vectorized::SeekRange> _tablet_seek_ranges;
    Rowset* _largest_rowset = nullptr;
    SegmentGroupPtr _segment_group = nullptr;
    vectorized::SchemaPtr _short_key_schema = nullptr;
    int64_t _sample_splitted_scan_blocks = 0;

    std::vector<std::pair<ShortKeyIndexGroupIterator, ShortKeyIndexGroupIterator>> _block_ranges_per_seek_range;
    std::vector<size_t> _num_rest_blocks_per_seek_range;
    size_t _range_idx = 0;
    ShortKeyIndexGroupIterator _next_lower_block_iter;
};

MorselQueuePtr create_empty_morsel_queue();

} // namespace pipeline
} // namespace starrocks
