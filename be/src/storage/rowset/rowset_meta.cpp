// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

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
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->rowset_metadata_mem_tracker(), _mem_usage);
}

RowsetMeta::RowsetMeta(std::unique_ptr<RowsetMetaPB>& rowset_meta_pb) {
    _rowset_meta_pb = std::move(rowset_meta_pb);
    _init();
    _mem_usage = _calc_mem_usage();
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->rowset_metadata_mem_tracker(), _mem_usage);
}

RowsetMeta::RowsetMeta(const RowsetMetaPB& rowset_meta_pb) {
    _rowset_meta_pb = std::make_unique<RowsetMetaPB>(rowset_meta_pb);
    _init();
    _mem_usage = _calc_mem_usage();
    MEM_TRACKER_SAFE_CONSUME(ExecEnv::GetInstance()->rowset_metadata_mem_tracker(), _mem_usage);
}

RowsetMeta::~RowsetMeta() {
    MEM_TRACKER_SAFE_RELEASE(ExecEnv::GetInstance()->rowset_metadata_mem_tracker(), _mem_usage);
}

} // namespace starrocks
