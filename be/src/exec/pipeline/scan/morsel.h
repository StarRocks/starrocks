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

/// MorselQueue.
class MorselQueue {
public:
    MorselQueue() = default;
    virtual ~MorselQueue() = default;

    virtual std::vector<TInternalScanRange*> olap_scan_ranges() const = 0;

    virtual size_t num_morsels() const = 0;
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

    size_t num_morsels() const override { return _num_morsels; }
    bool empty() const override { return _pop_index >= _num_morsels; }
    StatusOr<MorselPtr> try_get() override;

    std::string name() const override { return "fixed_morsel_queue"; }

private:
    Morsels _morsels;
    const size_t _num_morsels;
    std::atomic<size_t> _pop_index;
};

} // namespace pipeline
} // namespace starrocks
