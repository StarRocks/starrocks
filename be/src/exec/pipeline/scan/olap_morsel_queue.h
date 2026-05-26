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

#include <memory>
#include <vector>

#include "exec/pipeline/scan/morsel_queue.h"
#include "storage/primitive/olap_tuple.h"
#include "storage/tablet_reader_params.h"
#include "storage/tablet_schema.h"

namespace starrocks {

struct OlapScanRange;
class BaseTablet;
using BaseTabletSharedPtr = std::shared_ptr<BaseTablet>;
class Segment;
using SegmentSharedPtr = std::shared_ptr<Segment>;

class SeekTuple;
struct ShortKeyOption;
using ShortKeyOptionPtr = std::unique_ptr<ShortKeyOption>;
class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace pipeline {

// Capability interface for queues that expose OLAP scan preparation hooks.
class OlapMorselQueue : public MorselQueue {
public:
    using MorselQueue::MorselQueue;
    ~OlapMorselQueue() override = default;

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
    virtual void set_tablet_schema(const TabletSchemaCSPtr& tablet_schema) {
        DCHECK(tablet_schema != nullptr);
        _tablet_schema = tablet_schema;
    }

protected:
    std::vector<BaseTabletSharedPtr> _tablets;
    std::vector<std::vector<BaseRowsetSharedPtr>> _tablet_rowsets;
    TabletSchemaCSPtr _tablet_schema = nullptr;
};

} // namespace pipeline
} // namespace starrocks
