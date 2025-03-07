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

#include "exec/multi_olap_table_sink.h"

namespace starrocks {

MultiOlapTableSink::MultiOlapTableSink(ObjectPool* pool, const std::vector<TExpr>& texprs)
        : _pool(pool), _texprs(texprs) {}

Status MultiOlapTableSink::init(const TDataSink& sink, RuntimeState* state) {
    Status status;
    for (const auto& olap_table_sink : sink.multi_olap_table_sinks) {
        auto olap_sink = std::make_unique<OlapTableSink>(_pool, _texprs, &status, state);
        RETURN_IF_ERROR(status);
        RETURN_IF_ERROR(olap_sink->init(olap_table_sink, state));
        _sinks.emplace_back(std::move(olap_sink));
    }

    return status;
}

Status MultiOlapTableSink::prepare(RuntimeState* state) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->prepare(state));
    }
    return Status::OK();
}

Status MultiOlapTableSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->send_chunk(state, chunk));
    }
    return Status::OK();
}

Status MultiOlapTableSink::open(RuntimeState* state) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->open(state));
    }
    return Status::OK();
}

Status MultiOlapTableSink::try_open(RuntimeState* state) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->try_open(state));
    }
    return Status::OK();
}

bool MultiOlapTableSink::is_open_done() {
    for (auto& sink : _sinks) {
        if (!sink->is_open_done()) {
            return false;
        }
    }
    return true;
}

Status MultiOlapTableSink::open_wait() {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->open_wait());
    }
    return Status::OK();
}

Status MultiOlapTableSink::send_chunk_nonblocking(RuntimeState* state, Chunk* chunk) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->send_chunk_nonblocking(state, chunk));
    }
    return Status::OK();
}

bool MultiOlapTableSink::is_full() {
    for (auto& sink : _sinks) {
        if (sink->is_full()) {
            return true;
        }
    }
    return false;
}

Status MultiOlapTableSink::try_close(RuntimeState* state) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->try_close(state));
    }
    return Status::OK();
}

Status MultiOlapTableSink::close_wait(RuntimeState* state, Status close_status) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->close_wait(state, close_status));
    }
    return Status::OK();
}

bool MultiOlapTableSink::is_close_done() {
    for (auto& sink : _sinks) {
        if (!sink->is_close_done()) {
            return false;
        }
    }
    return true;
}

Status MultiOlapTableSink::close(RuntimeState* state, Status close_status) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->close(state, close_status));
    }
    return Status::OK();
}

RuntimeProfile* MultiOlapTableSink::profile() {
    RuntimeProfile* profile = nullptr;
    for (auto& sink : _sinks) {
        if (profile == nullptr) {
            profile = sink->profile();
        } else {
            profile->merge(sink->profile());
        }
    }
    return profile;
}

Status MultiOlapTableSink::reset_epoch(RuntimeState* state) {
    for (auto& sink : _sinks) {
        RETURN_IF_ERROR(sink->reset_epoch(state));
    }
    return Status::OK();
}

void MultiOlapTableSink::add_olap_table_sink(std::unique_ptr<OlapTableSink> sink) {
    _sinks.emplace_back(std::move(sink));
}

std::unique_ptr<OlapTableSink>& MultiOlapTableSink::get_olap_table_sink(int index) {
    return _sinks[index];
}

void MultiOlapTableSink::set_profile(RuntimeProfile* profile) {
    for (auto& sink : _sinks) {
        sink->set_profile(profile);
    }
}

} // namespace starrocks
