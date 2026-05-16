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
#include <vector>

#include "exec/pipeline/scan/morsel_queue.h"
#include "gen_cpp/InternalService_types.h"
#include "glog/logging.h"
#include "storage/tablet_reader_params.h"
#include "storage/tablet_schema.h"

namespace starrocks {

class BaseTablet;
using BaseTabletSharedPtr = std::shared_ptr<BaseTablet>;
struct OlapScanRange;

namespace pipeline {

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

// The morsel queue with a fixed number of morsels, which is determined in the constructor.
class FixedMorselQueue final : public OlapMorselQueue {
public:
    explicit FixedMorselQueue(Morsels&& morsels) : OlapMorselQueue(std::move(morsels)), _pop_index(0) {}
    ~FixedMorselQueue() override = default;
    bool empty() const override { return _unget_morsel == nullptr && _pop_index >= _num_morsels; }
    StatusOr<MorselPtr> try_get() override;

    std::string name() const override { return "fixed_morsel_queue"; }
    Type type() const override { return FIXED; }

private:
    std::atomic<size_t> _pop_index;
};

} // namespace pipeline
} // namespace starrocks
