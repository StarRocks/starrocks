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

#include "storage/delta_column_group.h"

#include <memory>

#include "gen_cpp/olap_common.pb.h"
#include "storage/rowset/rowset.h"

namespace starrocks {

void DeltaColumnGroup::init(int64_t version, const std::vector<uint32_t>& column_ids, const std::string& column_file) {
    _version = version;
    _column_ids = column_ids;
    _column_file = column_file;
    _calc_memory_usage();
}

void DeltaColumnGroup::_calc_memory_usage() {
    _memory_usage = sizeof(size_t) + sizeof(int64_t) + sizeof(uint32_t) * _column_ids.size() + _column_file.length();
}

Status DeltaColumnGroup::load(int64_t version, const char* data, size_t length) {
    _version = version;
    DeltaColumnGroupPB dcg_pb;
    if (!dcg_pb.ParseFromArray(data, length)) {
        return Status::Corruption("DeltaColumnGroup load failed");
    }
    _column_file = dcg_pb.column_file();
    for (uint32_t cid : dcg_pb.column_ids()) {
        _column_ids.push_back(cid);
    }
    _calc_memory_usage();
    return Status::OK();
}

std::string DeltaColumnGroup::save() const {
    DeltaColumnGroupPB dcg_pb;
    dcg_pb.set_column_file(_column_file);
    for (uint32_t cid : _column_ids) {
        dcg_pb.add_column_ids(cid);
    }
    std::string result;
    dcg_pb.SerializeToString(&result);
    return result;
}

std::string DeltaColumnGroupListSerializer::serialize_delta_column_group_list(const DeltaColumnGroupList& dcgs) {
    DeltaColumnGroupListPB dcgs_pb;
    for (const auto& dcg : dcgs) {
        dcgs_pb.add_versions(dcg->version());
        DeltaColumnGroupPB dcg_pb;
        dcg_pb.set_column_file(dcg->relative_column_file());
        for (uint32_t cid : dcg->column_ids()) {
            dcg_pb.add_column_ids(cid);
        }
        dcgs_pb.add_dcgs()->CopyFrom(dcg_pb);
    }
    std::string result;
    dcgs_pb.SerializeToString(&result);
    return result;
}

Status DeltaColumnGroupListSerializer::deserialize_delta_column_group_list(const char* data, size_t length,
                                                                           DeltaColumnGroupList* dcgs) {
    DeltaColumnGroupListPB dcgs_pb;
    if (!dcgs_pb.ParseFromArray(data, length)) {
        return Status::Corruption("parse delta column group failed");
    }
    DCHECK(dcgs_pb.versions_size() == dcgs_pb.dcgs_size());
    for (int i = 0; i < dcgs_pb.versions_size(); i++) {
        auto dcg = std::make_shared<DeltaColumnGroup>();
        std::vector<uint32_t> column_ids;
        for (uint32_t cid : dcgs_pb.dcgs(i).column_ids()) {
            column_ids.push_back(cid);
        }
        dcg->init(dcgs_pb.versions(i), column_ids, dcgs_pb.dcgs(i).column_file());
        dcgs->push_back(dcg);
    }
    return Status::OK();
}

} // namespace starrocks