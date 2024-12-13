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

#include "exec/data_sink.h"

namespace starrocks {

class Status;

// This class is a sinker for broadcasting the dictionary cache
// to all BE node.
// It is just a interface to create the pipeline operator.
class DictionaryCacheSink : public DataSink {
public:
    DictionaryCacheSink() = default;

    ~DictionaryCacheSink() override = default;

    Status init(const TDataSink& thrift_sink, RuntimeState* state) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send_chunk(RuntimeState* state, Chunk* chunk) override;

    RuntimeProfile* profile() override { return nullptr; }
};

} // end namespace starrocks
