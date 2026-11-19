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
#include <mutex>
#include <vector>

#include "base/brpc/ref_count_closure.h"
#include "column/vectorized_fwd.h"
#include "common/thread/threadpool.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_fwd.h"

namespace starrocks {

// The nodes that failed during one refresh, shared by every sink driver of the fragment. With one set per
// driver, two drivers that lose different nodes would each go on, and every node would end up with part of
// the data.
class DictionaryCacheFailedNodes {
public:
    explicit DictionaryCacheFailedNodes(size_t num_nodes) : _failed(num_nodes, false) {}

    bool is_failed(size_t node);
    // Returns true if no node is left. Sets *newly_failed if this call is the one that marked the node.
    bool mark_failed(size_t node, bool* newly_failed);

private:
    std::mutex _lock;
    std::vector<bool> _failed;
    size_t _num_failed = 0;
};

class DictionaryCacheWriter {
public:
    // Without `failed_nodes`, the writer keeps a set of its own.
    DictionaryCacheWriter(const TDictionaryCacheSink& t_dictionary_cache_sink, RuntimeState* state,
                          starrocks::pipeline::FragmentContext* fragment_ctx,
                          std::shared_ptr<DictionaryCacheFailedNodes> failed_nodes = nullptr);
    ~DictionaryCacheWriter() = default;

    Status prepare();

    Status append_chunk(const ChunkPtr& chunk, std::atomic_bool* terminate_flag = nullptr);

    bool need_input();

    Status set_finishing();

    bool is_finished();

    Status cancel();

    void close();

    void sync_dictionary_cache(const Chunk* chunk);

private:
    Status _send_request(ChunkPB* pchunk, POlapTableSchemaParam* pschema,
                         std::vector<RefCountClosure<PProcessDictionaryCacheResult>*>& closures,
                         std::vector<bool>& sent);

    Status _wait_response(std::vector<RefCountClosure<PProcessDictionaryCacheResult>*>& closures,
                          const std::vector<bool>& sent);

    bool _skip_failed_nodes() const;
    bool _is_node_failed(size_t node);
    // Returns true if no node is left.
    bool _mark_node_failed(size_t node, const Status& st);

    Status _submit();

    const TDictionaryCacheSink _t_dictionary_cache_sink;
    std::atomic_bool _is_prepared = false;
    std::atomic_bool _is_cancelled = false;
    std::atomic_bool _is_finished = false;
    std::atomic_int64_t _num_pending_tasks = 0;

    ChunkUniquePtr _buffer_chunk = nullptr;
    ChunkUniquePtr _immutable_buffer_chunk = nullptr;

    // control max size of chunk whether we should begin a refresh task
    static const long kChunkBufferLimit = 16 * 1024 * 1024; // 16MB
    // control max memory useage in current writer
    static const long kMaxMemoryUsageLimit = 256 * 1024 * 1024; // 256MB

    RuntimeState* _state = nullptr;
    starrocks::pipeline::FragmentContext* _fragment_ctx;

    // With skip_failed_nodes, a node that fails gets no more chunks. The FE then finds that it has fewer
    // rows and leaves it out at COMMIT.
    std::shared_ptr<DictionaryCacheFailedNodes> _failed_nodes;
};

}; // namespace starrocks
