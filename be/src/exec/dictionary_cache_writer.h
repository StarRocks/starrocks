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

#include "column/chunk.h"
#include "exec/pipeline/fragment_context.h"
#include "gen_cpp/InternalService_types.h"
#include "gen_cpp/doris_internal_service.pb.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/runtime_state.h"
#include "util/reusable_closure.h"
#include "util/threadpool.h"

namespace starrocks {

class DictionaryCacheWriter {
public:
    DictionaryCacheWriter(const TDictionaryCacheSink& t_dictionary_cache_sink, RuntimeState* state,
                          starrocks::pipeline::FragmentContext* fragment_ctx);
    ~DictionaryCacheWriter() = default;

    Status prepare();

    Status append_chunk(ChunkPtr chunk, std::atomic_bool* terminate_flag = nullptr);

    bool need_input();

    Status set_finishing();

    bool is_finished();

    Status cancel();

    void close();

    void sync_dictionary_cache(const Chunk* chunk);

    class ChunkUtil {
    public:
        ChunkUtil() = default;
        ~ChunkUtil() = default;

        static Status compress_and_serialize_chunk(const Chunk* src, ChunkPB* dst);
        static Status uncompress_and_deserialize_chunk(const ChunkPB& pchunk, Chunk& chunk,
                                                       faststring* uncompressed_buffer,
                                                       const OlapTableSchemaParam* schema);
        static Status check_chunk_has_null(const Chunk& chunk);
    };

private:
    Status _send_request(ChunkPB* pchunk, POlapTableSchemaParam* pschema,
                         std::vector<RefCountClosure<PProcessDictionaryCacheResult>*>& closures);

    Status _wait_response(std::vector<RefCountClosure<PProcessDictionaryCacheResult>*>& closures);

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
};

}; // namespace starrocks