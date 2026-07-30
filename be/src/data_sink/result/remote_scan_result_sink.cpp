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

#include "data_sink/result/remote_scan_result_sink.h"

#include <utility>

namespace starrocks {

RemoteScanResultSink::RemoteScanResultSink(RecordDescriptor record_desc, const std::vector<TExpr>& output_exprs,
                                           TRemoteScanResultSink sink)
        : _record_desc(std::move(record_desc)),
          _t_output_expr(output_exprs),
          _sink(std::move(sink)),
          _profile(std::make_unique<RuntimeProfile>("RemoteScanResultSink")) {}

} // namespace starrocks
