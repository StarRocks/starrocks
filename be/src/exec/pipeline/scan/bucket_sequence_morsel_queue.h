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

namespace starrocks::pipeline {

class BucketSequenceMorselQueue : public OlapMorselQueue, public TicketedMorselQueue {
public:
    BucketSequenceMorselQueue(MorselQueuePtr&& morsel_queue);
    std::vector<TInternalScanRange*> prepare_olap_scan_ranges() const override;

    void set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) override {
        _olap_morsel_queue()->set_key_ranges(key_ranges);
    }

    void set_key_ranges(const TabletReaderParams::RangeStartOperation& range_start_op,
                        const TabletReaderParams::RangeEndOperation& range_end_op,
                        const std::vector<OlapTuple>& range_start_key,
                        const std::vector<OlapTuple>& range_end_key) override {
        _olap_morsel_queue()->set_key_ranges(range_start_op, range_end_op, range_start_key, range_end_key);
    }

    void set_tablets(const std::vector<BaseTabletSharedPtr>& tablets) override {
        _olap_morsel_queue()->set_tablets(tablets);
    }

    void set_tablet_rowsets(const std::vector<std::vector<BaseRowsetSharedPtr>>& tablet_rowsets) override {
        _olap_morsel_queue()->set_tablet_rowsets(tablet_rowsets);
    }

    void set_ticket_checker(const query_cache::TicketCheckerPtr& ticket_checker) override {
        _ticket_checker = ticket_checker;
    }
    bool could_attch_ticket_checker() const override { return true; }

    size_t num_original_morsels() const override { return _morsel_queue->num_original_morsels(); }
    size_t max_degree_of_parallelism() const override { return _morsel_queue->max_degree_of_parallelism(); }
    bool empty() const override;
    StatusOr<MorselPtr> try_get() override;
    std::string name() const override;
    StatusOr<bool> ready_for_next() const override;
    Status append_morsels(Morsels&& morsels) override { return _morsel_queue->append_morsels(std::move(morsels)); }
    Type type() const override { return BUCKET_SEQUENCE; }

    void set_tablet_schema(const TabletSchemaCSPtr& tablet_schema) override {
        OlapMorselQueue::set_tablet_schema(tablet_schema);
        _olap_morsel_queue()->set_tablet_schema(tablet_schema);
    }

private:
    OlapMorselQueue* _olap_morsel_queue() const {
        auto* olap_morsel_queue = dynamic_cast<OlapMorselQueue*>(_morsel_queue.get());
        DCHECK(olap_morsel_queue != nullptr);
        return olap_morsel_queue;
    }

    StatusOr<int64_t> _peek_sequence_id() const;
    mutable std::mutex _mutex;

    MorselQueuePtr _morsel_queue;
    query_cache::TicketCheckerPtr _ticket_checker;
    int64_t _current_sequence = -1;
};

} // namespace starrocks::pipeline
