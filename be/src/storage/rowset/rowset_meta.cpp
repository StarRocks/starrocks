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

#include "storage/rowset/rowset_meta.h"

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"

namespace starrocks {
RowsetMeta::RowsetMeta(std::string_view pb_rowset_meta, bool* parse_ok) {
    _rowset_meta_pb = std::make_unique<RowsetMetaPB>();
    *parse_ok = _deserialize_from_pb(pb_rowset_meta);
    if (*parse_ok) {
        _init();
    }
    _mem_usage = _calc_mem_usage();
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->rowset_metadata_mem_tracker(), _mem_usage);
}

RowsetMeta::RowsetMeta(std::unique_ptr<RowsetMetaPB>& rowset_meta_pb) {
    _rowset_meta_pb = std::move(rowset_meta_pb);
    _init();
    _mem_usage = _calc_mem_usage();
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->rowset_metadata_mem_tracker(), _mem_usage);
}

RowsetMeta::RowsetMeta(const RowsetMetaPB& rowset_meta_pb) {
    _rowset_meta_pb = std::make_unique<RowsetMetaPB>(rowset_meta_pb);
    _init();
    _mem_usage = _calc_mem_usage();
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->rowset_metadata_mem_tracker(), _mem_usage);
}

RowsetMeta::~RowsetMeta() {
    MEM_TRACKER_SAFE_RELEASE(GlobalEnv::GetInstance()->rowset_metadata_mem_tracker(), _mem_usage);
}

} // namespace starrocks
