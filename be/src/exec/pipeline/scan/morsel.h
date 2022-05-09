// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

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
    PhysicalSplitScanMorsel(int32_t plan_node_id, const TScanRange& scan_range, std::string&& rowset_id,
                            uint64_t segment_id, vectorized::SparseRange&& rowid_range)
            : ScanMorsel(plan_node_id, scan_range),
              _rowset_id(std::move(rowset_id)),
              _segment_id(segment_id),
              _rowid_range(std::move(rowid_range)) {}

    void init_tablet_reader_params(vectorized::TabletReaderParams* params) override;

private:
    std::string _rowset_id;
    uint64_t _segment_id;
    vectorized::SparseRange _rowid_range;
};

/// MorselQueue.
class MorselQueue {
public:
    MorselQueue() = default;
    virtual ~MorselQueue() = default;

    virtual std::vector<TInternalScanRange*> olap_scan_ranges() const = 0;

    virtual void set_key_ranges(const std::vector<OlapScanRange*>& key_ranges) {}
    virtual void set_tablets(const std::vector<TabletSharedPtr>& tablets) {}
    virtual void set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) {}

    virtual size_t num_morsels() const = 0;
    virtual bool empty() const = 0;
    virtual StatusOr<MorselPtr> try_get() = 0;
};

class FixedMorselQueue final : public MorselQueue {
public:
    explicit FixedMorselQueue(Morsels&& morsels)
            : _morsels(std::move(morsels)), _num_morsels(_morsels.size()), _pop_index(0) {}
    ~FixedMorselQueue() override = default;

    std::vector<TInternalScanRange*> olap_scan_ranges() const override;

    size_t num_morsels() const override { return _num_morsels; }
    bool empty() const override { return _pop_index >= _num_morsels; }
    StatusOr<MorselPtr> try_get() override;

private:
    Morsels _morsels;
    const size_t _num_morsels;
    std::atomic<size_t> _pop_index;
};

class PhysicalSplitMorselQueue final : public MorselQueue {
public:
    explicit PhysicalSplitMorselQueue(Morsels&& morsels) : _morsels(std::move(morsels)) {}
    ~PhysicalSplitMorselQueue() override = default;

    std::vector<TInternalScanRange*> olap_scan_ranges() const override;

    void set_key_ranges(const std::vector<OlapScanRange*>& key_ranges) override;
    void set_tablets(const std::vector<TabletSharedPtr>& tablets) override { _tablets = tablets; }
    void set_tablet_rowsets(const std::vector<std::vector<RowsetSharedPtr>>& tablet_rowsets) override {
        _tablet_rowsets = tablet_rowsets;
    }

    size_t num_morsels() const override { return std::numeric_limits<size_t>::max(); }
    bool empty() const override { return _tablet_idx >= _tablets.size(); }
    StatusOr<MorselPtr> try_get() override;

private:
    static rowid_t _lower_bound_ordinal(Segment* segment, const vectorized::SeekTuple& key, bool lower);
    static rowid_t _upper_bound_ordinal(Segment* segment, const vectorized::SeekTuple& key, bool lower, rowid_t end);

    Status _init_tablet_scan_range();

    ScanMorsel* _cur_scan_morsel();
    BetaRowset* _cur_rowset();
    Segment* _cur_segment();
    void _move_forward();

private:
    static constexpr int64_t UNINITIALIZED_SEGMENT = -1;

    const int64_t _min_scan_rows = config::pipeline_min_scan_rows;

    std::mutex _mutex;

    const Morsels _morsels;

    // possible values are "gt", "ge", "eq"
    std::string _range_start_op;
    // possible values are "lt", "le"
    std::string _range_end_op;
    std::vector<OlapTuple> _range_start_key;
    std::vector<OlapTuple> _range_end_key;

    std::vector<TabletSharedPtr> _tablets;
    std::vector<std::vector<RowsetSharedPtr>> _tablet_rowsets;

    std::atomic<size_t> _tablet_idx = 0;
    size_t _rowset_idx = 0;
    size_t _segment_idx = 0;
    // -1 means this segment hasn't been initiated.
    int64_t _num_segment_rest_rows = UNINITIALIZED_SEGMENT;
    std::vector<vectorized::SeekRange> _tablet_seek_ranges;
    vectorized::SparseRange _segment_scan_range;
    vectorized::SparseRangeIterator _segment_range_iter;

    MemPool _mempool;
};

} // namespace pipeline
} // namespace starrocks