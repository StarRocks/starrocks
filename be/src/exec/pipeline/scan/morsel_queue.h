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

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/statusor.h"
#include "exec/pipeline/scan/scan_morsel.h"
#include "exec/query_cache/ticket_checker.h"
#include "gutil/casts.h"
#include "runtime/mem_pool.h"
#include "storage/olap_common.h"
#include "storage/olap_tuple.h"
#include "storage/range.h"
#include "storage/rowset/segment_group.h"
#include "storage/seek_range.h"
#include "storage/tablet.h"
#include "storage/tablet_reader_params.h"

namespace starrocks {

struct OlapScanRange;
class BaseTablet;
using BaseTabletSharedPtr = std::shared_ptr<BaseTablet>;
class Segment;
using SegmentSharedPtr = std::shared_ptr<Segment>;

struct TabletReaderParams;
class SeekTuple;
struct ShortKeyOption;
using ShortKeyOptionPtr = std::unique_ptr<ShortKeyOption>;
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace pipeline {

class MorselQueue;
class SplitMorselQueue;
using MorselQueuePtr = std::unique_ptr<MorselQueue>;
using MorselQueueMap = std::unordered_map<int32_t, MorselQueuePtr>;
class MorselQueueFactory;
using MorselQueueFactoryPtr = std::unique_ptr<MorselQueueFactory>;
using MorselQueueFactoryMap = std::unordered_map<int32_t, MorselQueueFactoryPtr>;

/// MorselQueue.
class MorselQueue {
public:
    enum Type {
        FIXED,
        DYNAMIC,
        SPLIT,
        LOGICAL_SPLIT,
        PHYSICAL_SPLIT,
        BUCKET_SEQUENCE,
    };
    MorselQueue() = default;
    MorselQueue(Morsels&& morsels) : _morsels(std::move(morsels)), _num_morsels(_morsels.size()) {}
    virtual ~MorselQueue() = default;

    // NOTE: some subclasses of MorselQueue nest another MorselQueue, such as BucketSequenceMorselQueue.
    // When adding a new virtual method, DO NOT forget to invoke it on the nested MorselQueue as well.

    virtual std::vector<TInternalScanRange*> prepare_olap_scan_ranges() const;
    virtual void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) {}
    virtual void set_key_ranges(const TabletReaderParams::RangeStartOperation& range_start_op,
                                const TabletReaderParams::RangeEndOperation& range_end_op,
                                const std::vector<OlapTuple>& range_start_key,
                                const std::vector<OlapTuple>& range_end_key) {}
    virtual void set_tablets(const std::vector<BaseTabletSharedPtr>& tablets) { _tablets = tablets; }
    virtual void set_tablet_rowsets(const std::vector<std::vector<BaseRowsetSharedPtr>>& tablet_rowsets) {
        _tablet_rowsets = tablet_rowsets;
    }
    virtual void set_ticket_checker(const query_cache::TicketCheckerPtr& ticket_checker) {}
    virtual bool could_attch_ticket_checker() const { return false; }

    virtual size_t num_original_morsels() const { return _num_morsels; }
    virtual size_t max_degree_of_parallelism() const { return _num_morsels; }
    virtual bool empty() const = 0;
    virtual StatusOr<MorselPtr> try_get() = 0;
    virtual void unget(MorselPtr&& morsel);
    virtual std::string name() const = 0;
    virtual StatusOr<bool> ready_for_next() const { return true; }
    virtual Status append_morsels(Morsels&& morsels);
    virtual Type type() const = 0;
    virtual void set_tablet_schema(const TabletSchemaCSPtr& tablet_schema) {
        DCHECK(tablet_schema != nullptr);
        _tablet_schema = tablet_schema;
    }
    // is there any more scan ranges delivered from FE to be processed?
    bool has_more() const { return _has_more_scan_ranges || _has_more_from_split; }
    bool has_more_scan_ranges() const { return _has_more_scan_ranges; }
    bool has_more_from_split() const { return _has_more_from_split; }
    void set_has_more_scan_ranges(bool v) { _has_more_scan_ranges = v; }
    void set_has_more_from_split(bool v) { _has_more_from_split = v; }
    // do scan operator emit enough rows that we can stop processing scan ranges?
    void set_reach_limit(bool v) { _reach_limit = v; }
    bool reach_limit() const { return _reach_limit; }

protected:
    std::atomic<bool> _has_more_scan_ranges = false;
    std::atomic<bool> _has_more_from_split = false;
    std::atomic<bool> _reach_limit = false;
    Morsels _morsels;
    size_t _num_morsels = 0;
    MorselPtr _unget_morsel = nullptr;
    std::vector<BaseTabletSharedPtr> _tablets;
    std::vector<std::vector<BaseRowsetSharedPtr>> _tablet_rowsets;
    TabletSchemaCSPtr _tablet_schema = nullptr;
};

} // namespace pipeline
} // namespace starrocks
