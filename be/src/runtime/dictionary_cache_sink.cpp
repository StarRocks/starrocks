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

#include "runtime/dictionary_cache_sink.h"

#include "runtime/runtime_state.h"

namespace starrocks {

Status DictionaryCacheSink::init(const TDataSink& t_sink, RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::init(t_sink, state));
    return Status::OK();
}

Status DictionaryCacheSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    return Status::OK();
}

Status DictionaryCacheSink::open(RuntimeState* state) {
    return Status::NotSupported("dictionary cache only support pipeline engine");
}

Status DictionaryCacheSink::send_chunk(RuntimeState* state, Chunk* chunk) {
    return Status::NotSupported("dictionary cache only support pipeline engine");
}

} // namespace starrocks
