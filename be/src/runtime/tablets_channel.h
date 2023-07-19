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
class PTabletWriterAddBatchResult;
class PTabletWriterAddChunkRequest;
class PTabletWriterAddSegmentRequest;
class PTabletWriterAddSegmentResult;

class TabletsChannelOpenTimeStat {
public:
    TabletsChannelOpenTimeStat() = default;

    int64_t get_start_time() { return _start_time_ns; }

    void set_start_time(int64_t start_time_ns) { _start_time_ns = start_time_ns; }

    int64_t get_end_time() { return _end_time_ns; }

    void set_end_time(int64_t end_time_ns) { _end_time_ns = end_time_ns; }

    int64_t get_total_time() { return _end_time_ns - _start_time_ns; }

    void add_open_writer_cost(int64_t cost_ns) {
        _open_writer_cost_ns += cost_ns;
        _num_writers += 1;
    }

    std::string to_string();

protected:
    int64_t _start_time_ns;
    int64_t _end_time_ns;
    int64_t _open_writer_cost_ns;
    int64_t _num_writers;
};

inline std::string TabletsChannelOpenTimeStat::to_string() {
    std::stringstream ss;
    ss << "TabletsChannel={start_time_ns=" << get_start_time() << ", num_writers=" << _num_writers
       << ", total_cost_ns=" << get_total_time() << ", open_writer_cost_ns=" << _open_writer_cost_ns
       << ", other_cost_ns=" << (get_total_time() - _open_writer_cost_ns) << "}";
    return ss.str();
}

class TabletsChannel {
public:
    TabletsChannel() = default;
    virtual ~TabletsChannel() = default;

    [[nodiscard]] virtual Status open(const PTabletWriterOpenRequest& params,
                                      std::shared_ptr<OlapTableSchemaParam> schema, bool is_incremental,
                                      TabletsChannelOpenTimeStat* open_time_stat) = 0;

    virtual Status incremental_open(const PTabletWriterOpenRequest& params,
                                    std::shared_ptr<OlapTableSchemaParam> schema) = 0;

    virtual void add_chunk(Chunk* chunk, const PTabletWriterAddChunkRequest& request,
                           PTabletWriterAddBatchResult* response) = 0;

    virtual void cancel() = 0;

    virtual void abort() = 0;
};

struct TabletsChannelKey {
    UniqueId id;
    int64_t index_id;

    TabletsChannelKey(const PUniqueId& pid, int64_t index_id_) : id(pid), index_id(index_id_) {}

    ~TabletsChannelKey() noexcept = default;

    bool operator==(const TabletsChannelKey& rhs) const noexcept { return index_id == rhs.index_id && id == rhs.id; }

    [[nodiscard]] std::string to_string() const;
};

inline std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key) {
    os << "(id=" << key.id << ",index_id=" << key.index_id << ")";
    return os;
}

inline std::string TabletsChannelKey::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

} // namespace starrocks
