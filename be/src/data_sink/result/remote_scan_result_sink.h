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
#include <vector>

#include "common/runtime_profile.h"
#include "common/status.h"
#include "exec_primitive/data_sink.h"
#include "gen_cpp/DataSinks_types.h"
#include "gen_cpp/Exprs_types.h"
#include "runtime/descriptors.h"

namespace starrocks {

class RuntimeState;

// Pipeline-native sink for the StarRocks-catalog remote-scan data plane (source side).
// Execution is driven entirely by the RemoteScanResultSinkOperatorFactory built in
// decompose_data_sink_to_pipeline(); the non-pipeline DataSink hooks (open/send_chunk/
// profile) are unused. This subclass exists only so the sink flows through the same
// create_data_sink + decompose path as every other sink, instead of being special-cased.
class RemoteScanResultSink : public DataSink {
public:
    RemoteScanResultSink(const RowDescriptor& row_desc, const std::vector<TExpr>& output_exprs,
                         TRemoteScanResultSink sink);
    ~RemoteScanResultSink() override = default;

    Status open(RuntimeState* state) override { return Status::OK(); }
    RuntimeProfile* profile() override { return _profile.get(); }

    const RowDescriptor& get_row_desc() const { return _row_desc; }
    const std::vector<TExpr>& get_output_expr() const { return _t_output_expr; }
    const TRemoteScanResultSink& get_sink() const { return _sink; }

private:
    // Owned by the RuntimeState / plan, outlives this sink.
    const RowDescriptor& _row_desc;
    const std::vector<TExpr>& _t_output_expr;
    TRemoteScanResultSink _sink;
    std::unique_ptr<RuntimeProfile> _profile;
};

} // namespace starrocks
