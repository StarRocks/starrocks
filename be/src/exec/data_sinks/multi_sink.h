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

#include "exec_primitive/data_sink.h"
#include "runtime/runtime_state.h"

namespace starrocks {

// Option X (CK-compatible logical-sink MV JOIN/agg): marker DataSink on the collector fragment. The real work
// lives in DataSink::decompose_data_sink_to_pipeline's MULTI_SINK branch, which reads TMultiSink.branches and
// caps each stashed ExchangeSource pipeline (produced by MultiSinkDispatchNode) with its own OlapTableSink.
// This sink is never opened/executed directly (the fragment is fully decomposed into the N branch pipelines).
class MultiSink final : public DataSink {
public:
    MultiSink() = default;
    ~MultiSink() override = default;

    Status open(RuntimeState* state) override { return Status::OK(); }
    RuntimeProfile* profile() override { return nullptr; }
};

} // namespace starrocks
