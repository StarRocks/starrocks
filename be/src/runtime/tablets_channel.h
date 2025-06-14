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

#include <bthread/mutex.h>

#include <atomic>
#include <set>
#include <sstream>
#include <string>

#include "column/chunk.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/types.pb.h"
#include "gutil/ref_counted.h"
#include "util/uid_util.h"

namespace brpc {
class Controller;
}

namespace google::protobuf {
class Closure;
};

namespace starrocks {

class LoadChannel;
class OlapTableSchemaParam;
class PTabletWriterOpenRequest;
class PTabletWriterOpenResult;
class PTabletWriterAddBatchResult;
class PTabletWriterAddChunkRequest;
class PTabletWriterAddSegmentRequest;
class PTabletWriterAddSegmentResult;
class TableMetrics;

class TabletsChannel {
public:
    TabletsChannel() = default;
    virtual ~TabletsChannel() = default;

    virtual Status open(const PTabletWriterOpenRequest& params, PTabletWriterOpenResult* result,
                        std::shared_ptr<OlapTableSchemaParam> schema, bool is_incremental) = 0;

    virtual Status incremental_open(const PTabletWriterOpenRequest& params, PTabletWriterOpenResult* result,
                                    std::shared_ptr<OlapTableSchemaParam> schema) = 0;

    virtual void add_chunk(Chunk* chunk, const PTabletWriterAddChunkRequest& request,
                           PTabletWriterAddBatchResult* response, bool* close_channel_ptr) = 0;

    virtual void cancel() = 0;

    virtual void abort() = 0;

    virtual void abort(const std::vector<int64_t>& tablet_ids, const std::string& reason) = 0;

    // timeout: in microseconds
    virtual bool drain_senders(int64_t timeout, const std::string& log_msg);

    virtual void update_profile() = 0;

protected:
    bool _is_immutable_partition_empty() const {
        std::lock_guard l(_immutable_partition_ids_lock);
        return _immutable_partition_ids.empty();
    }

    bool _has_immutable_partition(int64_t partition_id) const {
        std::lock_guard l(_immutable_partition_ids_lock);
        return _immutable_partition_ids.count(partition_id) > 0;
    }

    void _insert_immutable_partition(int64_t partition_id) {
        std::lock_guard l(_immutable_partition_ids_lock);
        _immutable_partition_ids.insert(partition_id);
    }

    // counter of remaining senders
    std::atomic<int> _num_remaining_senders = 0;

    // counter of initial senders
    std::atomic<int> _num_initial_senders = 0;

    std::unordered_map<int64_t, std::atomic<int>> _tablet_id_to_num_remaining_senders;

    mutable bthread::Mutex _immutable_partition_ids_lock;
    std::set<int64_t> _immutable_partition_ids;
    std::shared_ptr<TableMetrics> _table_metrics;
};

struct TabletsChannelKey {
    UniqueId id;
    int64_t sink_id;
    int64_t index_id;

    TabletsChannelKey(const PUniqueId& pid, int64_t sink_id_, int64_t index_id_)
            : id(pid), sink_id(sink_id_), index_id(index_id_) {}

    ~TabletsChannelKey() noexcept = default;

    bool operator==(const TabletsChannelKey& rhs) const noexcept {
        return index_id == rhs.index_id && id == rhs.id && sink_id == rhs.sink_id;
    }

    bool operator<(const TabletsChannelKey& rhs) const noexcept {
        if (id != rhs.id) {
            return id < rhs.id;
        }
        if (sink_id != rhs.sink_id) {
            return sink_id < rhs.sink_id;
        }
        return index_id < rhs.index_id;
    }

    [[nodiscard]] std::string to_string() const;
};

inline std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key) {
    os << "(id=" << key.id << ",sink_id=" << key.sink_id << ",index_id=" << key.index_id << ")";
    return os;
}

inline std::string TabletsChannelKey::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

} // namespace starrocks
