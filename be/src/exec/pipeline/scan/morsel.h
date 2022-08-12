// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <atomic>
#include <optional>

#include "gen_cpp/InternalService_types.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/seek_range.h"
#include "storage/tuple.h"

namespace starrocks {

struct OlapScanRange;
class Tablet;
using TabletSharedPtr = std::shared_ptr<Tablet>;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class BetaRowset;
class Segment;

namespace vectorized {
class TabletReaderParams;
class SeekTuple;
struct RowidRangeOption;
using RowidRangeOptionPtr = std::shared_ptr<RowidRangeOption>;
} // namespace vectorized

namespace pipeline {

class Morsel;
using MorselPtr = std::unique_ptr<Morsel>;
using Morsels = std::vector<MorselPtr>;
class MorselQueue;
using MorselQueuePtr = std::unique_ptr<MorselQueue>;
using MorselQueueMap = std::unordered_map<int32_t, MorselQueuePtr>;

/// Morsel.
class Morsel {
public:
    Morsel(int32_t plan_node_id) : _plan_node_id(plan_node_id) {}
    virtual ~Morsel() = default;

    int32_t get_plan_node_id() const { return _plan_node_id; }

    virtual void init_tablet_reader_params(vectorized::TabletReaderParams* params) {}

private:
    int32_t _plan_node_id;
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
    virtual StatusOr<MorselPtr> try_get(int driver_seq) = 0;

    virtual std::string name() const = 0;

    virtual bool need_rebalance() const { return false; }
};

// The morsel queue with a fixed number of morsels, which is determined in the constructor.
class FixedMorselQueue final : public MorselQueue {
public:
    explicit FixedMorselQueue(Morsels&& morsels, int dop);
    ~FixedMorselQueue() override = default;

    std::vector<TInternalScanRange*> olap_scan_ranges() const override;

    size_t num_original_morsels() const override { return _num_morsels; }
    size_t max_degree_of_parallelism() const override { return _num_morsels; }
    bool empty() const override;
    StatusOr<MorselPtr> try_get(int driver_seq) override;

    std::string name() const override { return "fixed_morsel_queue"; }

private:
    Morsels _morsels;
    const size_t _num_morsels;
    std::atomic<size_t> _pop_index;

    bool _assign_morsels = false;
    int _scan_dop;
    std::map<int, Morsels> _morsels_per_operator;
    std::map<int, std::atomic_size_t> _next_morsel_index_per_operator;
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
    StatusOr<MorselPtr> try_get(int driver_seq) override;

    std::string name() const override { return "physical_split_morsel_queue"; }

    // Whether DOP after splitting tablets is greater than the number of tablets.
    // Only used by broadcast HashJoinProbeOperator. It will insert local exchange
    // to balance the chunk from scan node and increase parallelism of the probe operators,
    // if the number of tablets is less than DOP.
    bool need_rebalance() const override { return _num_original_morsels < _degree_of_parallelism; }

private:
    static rowid_t _lower_bound_ordinal(Segment* segment, const vectorized::SeekTuple& key, bool lower);
    static rowid_t _upper_bound_ordinal(Segment* segment, const vectorized::SeekTuple& key, bool lower, rowid_t end);

    ScanMorsel* _cur_scan_morsel();
    BetaRowset* _cur_rowset();
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
    // Possible values are "gt", "ge", "eq".
    std::string _range_start_op = "";
    // Possible values are "lt", "le".
    std::string _range_end_op = "";
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

} // namespace pipeline
} // namespace starrocks
