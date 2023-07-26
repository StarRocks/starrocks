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
#include "storage/protobuf_file.h"
#include "storage/rowset/rowset.h"
#include "storage/utils.h"

namespace starrocks {

void DeltaColumnGroup::init(int64_t version, const std::vector<std::vector<uint32_t>>& column_ids,
                            const std::vector<std::string>& column_files) {
    _version = version;
    _column_ids = column_ids;
    _column_files = column_files;
    _calc_memory_usage();
}

void DeltaColumnGroup::_calc_memory_usage() {
    size_t total_ids = 0;
    size_t total_column_name_size = 0;

    for (int i = 0; i < _column_ids.size(); ++i) {
        total_ids += _column_ids[i].size();
        total_column_name_size += _column_files[i].length();
    }

    _memory_usage = sizeof(size_t) + sizeof(int64_t) + sizeof(uint32_t) * total_ids + total_column_name_size;
}

int DeltaColumnGroup::merge_into_by_version(DeltaColumnGroupList& dcgs, const std::string& dir,
                                            const RowsetId& rowset_id, int segment_id) {
    int merged = 0;
    for (auto& dcg : dcgs) {
        if (dcg->merge_by_version(*this, dir, rowset_id, segment_id)) {
            ++merged;
        }
    }
    return merged;
}

bool DeltaColumnGroup::merge_by_version(DeltaColumnGroup& dcg, const std::string& dir, const RowsetId& rowset_id,
                                        int segment_id) {
    if (_version != dcg.version()) {
        return false;
    }

    size_t orig_size = _column_files.size();

    _column_ids.insert(_column_ids.end(), dcg.column_ids().begin(), dcg.column_ids().end());

    _column_files.resize(_column_files.size() + dcg.relative_column_files().size());
    // update the file name suffix to finish merge
    for (size_t suffix = orig_size; suffix < _column_files.size(); ++suffix) {
        _column_files[suffix] =
                file_name(Rowset::delta_column_group_path(dir, rowset_id, segment_id, _version, suffix));
    }

    _calc_memory_usage();
    return true;
}

Status DeltaColumnGroup::load(int64_t version, const char* data, size_t length) {
    _version = version;
    // It must be compatible with the old dcg metadata format, the basic idea
    // is following:
    // 1. remain the dcg metadata format definition called OldDeltaColumnGroupPB
    // 2. parse the meta data using OldDeltaColumnGroupPB if it fail in DeltaColumnGroupPB
    // 3. assign DeltaColumnGroupPB using OldDeltaColumnGroupPB

    OldDeltaColumnGroupPB old_dcg_pb;
    DeltaColumnGroupPB dcg_pb;

    bool parsed = dcg_pb.ParseFromArray(data, length);
    bool old_parsed = false;

    if (!parsed && !old_dcg_pb.ParseFromArray(data, length)) {
        return Status::Corruption("DeltaColumnGroup load failed");
    } else if (!parsed) {
        old_parsed = true;
    }

    CHECK((parsed && !old_parsed) || (!parsed && old_parsed));

    if (!parsed && old_parsed) {
        auto column_ids = dcg_pb.add_column_ids();
        for (const auto& id : old_dcg_pb.column_ids()) {
            column_ids->add_column_ids(id);
        }

        dcg_pb.add_column_files(old_dcg_pb.column_file());
    }

    for (const auto& column_file : dcg_pb.column_files()) {
        _column_files.push_back(column_file);
    }
    for (const auto& cids : dcg_pb.column_ids()) {
        _column_ids.emplace_back(std::vector<uint32_t>());
        for (const auto& cid : cids.column_ids()) {
            _column_ids.back().push_back(cid);
        }
    }
    _calc_memory_usage();
    return Status::OK();
}

std::string DeltaColumnGroup::save() const {
    DeltaColumnGroupPB dcg_pb;
    for (const auto& column_file : _column_files) {
        dcg_pb.add_column_files(column_file);
    }
    for (const auto& cids : _column_ids) {
        auto* dcg_col_pb = dcg_pb.add_column_ids();
        for (const auto& cid : cids) {
            dcg_col_pb->add_column_ids(cid);
        }
    }
    std::string result;
    dcg_pb.SerializeToString(&result);
    return result;
}

std::string DeltaColumnGroupListSerializer::serialize_delta_column_group_list(const DeltaColumnGroupList& dcgs,
                                                                              DeltaColumnGroupListPB* dst) {
    DeltaColumnGroupListPB dcgs_pb;
    for (const auto& dcg : dcgs) {
        dcgs_pb.add_versions(dcg->version());
        DeltaColumnGroupPB dcg_pb;
        for (const auto& relative_column_file : dcg->relative_column_files()) {
            dcg_pb.add_column_files(relative_column_file);
        }
        for (const auto& cids : dcg->column_ids()) {
            auto* dcg_col_pb = dcg_pb.add_column_ids();
            for (const auto& cid : cids) {
                dcg_col_pb->add_column_ids(cid);
            }
        }
        dcgs_pb.add_dcgs()->CopyFrom(dcg_pb);
    }

    if (dst != nullptr) {
        dst->CopyFrom(dcgs_pb);
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
    return _deserialize_delta_column_group_list(dcgs_pb, dcgs);
}

Status DeltaColumnGroupListSerializer::deserialize_delta_column_group_list(const DeltaColumnGroupListPB& dcgs_pb,
                                                                           DeltaColumnGroupList* dcgs) {
    return _deserialize_delta_column_group_list(dcgs_pb, dcgs);
}

Status DeltaColumnGroupListSerializer::_deserialize_delta_column_group_list(const DeltaColumnGroupListPB& dcgs_pb,
                                                                            DeltaColumnGroupList* dcgs) {
    DCHECK(dcgs_pb.versions_size() == dcgs_pb.dcgs_size());
    for (int i = 0; i < dcgs_pb.versions_size(); i++) {
        auto dcg = std::make_shared<DeltaColumnGroup>();
        std::vector<std::vector<uint32_t>> column_ids;
        std::vector<std::string> column_files;
        DCHECK(dcgs_pb.dcgs(i).column_ids().size() == dcgs_pb.dcgs(i).column_files().size());
        for (int j = 0; j < dcgs_pb.dcgs(i).column_ids().size(); ++j) {
            column_ids.emplace_back(std::vector<uint32_t>());
            for (const auto& cid : dcgs_pb.dcgs(i).column_ids(j).column_ids()) {
                column_ids.back().push_back(cid);
            }
            column_files.push_back(dcgs_pb.dcgs(i).column_files(j));
        }
        dcg->init(dcgs_pb.versions(i), column_ids, column_files);
        dcgs->push_back(dcg);
    }
    return Status::OK();
}

void DeltaColumnGroupListHelper::garbage_collection(DeltaColumnGroupList& dcg_list, TabletSegmentId tsid,
                                                    int64_t min_readable_version,
                                                    std::vector<std::pair<TabletSegmentId, int64_t>>& garbage_dcgs) {
    auto dcg_itr = dcg_list.begin();
    // The delta column group that need to be gc, should satisfy two point:
    // 1. It's version is not larger than min_readable_version
    // 2. It's all column are covered by other delta column groups which are also not larger than min_readable_version
    std::unordered_set<uint32_t> column_set;
    while (dcg_itr != dcg_list.end()) {
        if ((*dcg_itr)->version() > min_readable_version) {
            // skip the delta column group that isn't expired.
            dcg_itr++;
        } else {
            bool need_free = true;
            auto all_cids = (*dcg_itr)->column_ids();
            for (const auto& cids : all_cids) {
                for (uint32_t cid : cids) {
                    // We can't free the dcg that it's columns not in column_set yet.
                    if (column_set.count(cid) == 0) {
                        need_free = false;
                        break;
                    }
                }
            }
            if (need_free) {
                garbage_dcgs.push_back(std::make_pair(tsid, (*dcg_itr)->version()));
                dcg_itr = dcg_list.erase(dcg_itr);
            } else {
                for (const auto& cids : all_cids) {
                    for (uint32_t cid : cids) {
                        column_set.insert(cid);
                    }
                }
                dcg_itr++;
            }
        }
    }
}

Status DeltaColumnGroupListHelper::save_snapshot(const std::string& file_path,
                                                 DeltaColumnGroupSnapshotPB& dcg_snapshot_pb) {
    DCHECK(!file_path.empty());
    ProtobufFile file(file_path);
    return file.save(dcg_snapshot_pb, true);
}

Status DeltaColumnGroupListHelper::parse_snapshot(const std::string& file_path,
                                                  DeltaColumnGroupSnapshotPB& dcg_snapshot_pb) {
    ProtobufFile file(file_path);
    Status st = file.load(&dcg_snapshot_pb);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to load dcg meta file: " << st;
        return st;
    }
    return Status::OK();
}

} // namespace starrocks