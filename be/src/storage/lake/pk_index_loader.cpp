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

#include "storage/lake/pk_index_loader.h"

#include <fmt/format.h>

#include "common/constexpr.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "storage/chunk_helper.h"
#include "storage/lake/update_manager.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment_options.h"
#include "util/defer_op.h"

namespace starrocks::lake {

class PkSegmentScanTask {
public:
    PkSegmentScanTask(Tablet* tablet, uint32_t rowset_id, uint32_t seg_id, const std::string& seg_name,
                      const Schema& schema, int64_t version, const MetaFileBuilder* builder)
            : _tablet(tablet),
              _rowset_id(rowset_id),
              _seg_id(seg_id),
              _seg_name(std::move(seg_name)),
              _schema(std::move(schema)),
              _version(version),
              _builder(builder) {}

    void run() {
        auto st = Status::OK();
        DeferOp defer([&]() {
            if (st.is_end_of_file()) {
                st = Status::OK();
            }
            ExecEnv::GetInstance()->lake_pk_index_loader()->finish_subtask(_tablet->id(), st);
        });
        size_t footer_size_hint = 16 * 1024;
        auto seg = _tablet->load_segment(_seg_name, _seg_id, &footer_size_hint, false, false);
        if (!seg.ok()) {
            st = seg.status();
            return;
        }
        SegmentReadOptions seg_options;
        auto fs = FileSystem::CreateSharedFromString(_tablet->root_location());
        if (!fs.ok()) {
            st = fs.status();
            return;
        }
        OlapReaderStatistics stats;
        seg_options.fs = *fs;
        seg_options.stats = &stats;
        seg_options.is_primary_keys = true;
        seg_options.delvec_loader = std::make_shared<LakeDelvecLoader>(_tablet->update_mgr(), _builder);
        seg_options.version = _version;
        seg_options.tablet_id = _tablet->id();
        seg_options.rowset_id = _rowset_id;
        auto seg_itr = (*seg)->new_iterator(_schema, seg_options);
        if (!seg_itr.ok()) {
            st = seg_itr.status();
            return;
        }
        while (true) {
            auto chunk_info = std::make_shared<ChunkInfo>();
            chunk_info->rowids.reserve(DEFAULT_CHUNK_SIZE);
            chunk_info->chunk = ChunkHelper::new_chunk(_schema, DEFAULT_CHUNK_SIZE);
            chunk_info->rssid = _rowset_id + _seg_id;
            auto chunk = chunk_info->chunk.get();
            st = (*seg_itr)->get_next(chunk, &chunk_info->rowids);
            if (st.is_end_of_file()) {
                break;
            } else if (!st.ok()) {
                return;
            }
            ExecEnv::GetInstance()->lake_pk_index_loader()->add_chunk(_tablet->id(), chunk_info);
        }
        (*seg_itr)->close();
    }

private:
    Tablet* _tablet;
    uint32_t _rowset_id;
    uint32_t _seg_id;
    const std::string _seg_name;
    const Schema _schema;
    int64_t _version;
    const MetaFileBuilder* _builder;
};

PkIndexLoader::~PkIndexLoader() {
    _contexts.clear();
}

Status PkIndexLoader::load(Tablet* tablet, const std::vector<RowsetPtr>& rowsets, const Schema& schema, int64_t version,
                           const MetaFileBuilder* builder) {
    uint64_t subtask_num = 0;
    std::vector<std::shared_ptr<PkSegmentScanTask>> scan_tasks;
    for (auto& rowset : rowsets) {
        auto& rowset_meta = rowset->metadata();
        for (uint32_t i = 0; i < rowset_meta.segments_size(); ++i) {
            auto scan_task = std::make_shared<PkSegmentScanTask>(tablet, rowset->id(), i, rowset_meta.segments(i),
                                                                 schema, version, builder);
            bool ret = ExecEnv::GetInstance()->connector_scan_executor()->submit(
                    workgroup::ScanTask(workgroup::WorkGroupManager::instance()->get_default_workgroup().get(),
                                        [scan_task]() { scan_task->run(); }));
            if (!ret) {
                auto error_msg = fmt::format("Fail to submit pk segment scan subtask, tablet: {}", tablet->id());
                LOG(ERROR) << error_msg;
                return Status::IOError(error_msg);
            }
        }
        subtask_num += rowset_meta.segments_size();
    }

    std::lock_guard<std::mutex> lg(_mutex);
    auto context = std::make_shared<PkSegmentScanTaskContext>();
    context->subtask_num = subtask_num;
    _contexts.emplace(tablet->id(), context);
    return Status::OK();
}

void PkIndexLoader::add_chunk(int64_t tablet_id, const std::shared_ptr<ChunkInfo>& chunk) {
    std::lock_guard<std::mutex> lg(_mutex);
    auto it = _contexts.find(tablet_id);
    if (it == _contexts.end()) {
        return;
    } else {
        auto context = it->second;
        context->chunk_infos.push(chunk);
        context->cv.notify_all();
    }
}

StatusOr<std::shared_ptr<ChunkInfo>> PkIndexLoader::get_chunk(int64_t tablet_id) {
    std::unique_lock<std::mutex> lk(_mutex);
    auto it = _contexts.find(tablet_id);
    if (it == _contexts.end()) {
        auto error_msg = fmt::format("Invalid tablet id {}", tablet_id);
        return Status::NotFound(error_msg);
    }

    auto context = it->second;
    if (context->subtask_num == 0 && context->chunk_infos.empty()) {
        _contexts.erase(tablet_id);
        return Status::EndOfFile("done");
    }

    if (!context->status.is_ok_or_eof()) {
        Status st = context->status;
        _contexts.erase(tablet_id);
        return st;
    }

    if (!context->chunk_infos.empty()) {
        return context->get_chunk_info();
    }

    context->cv.wait(lk, [&] {
        return !context->chunk_infos.empty() || !context->status.is_ok_or_eof() || context->subtask_num == 0;
    });

    if (!context->status.is_ok_or_eof()) {
        Status st = context->status;
        _contexts.erase(tablet_id);
        return st;
    }
    if (context->subtask_num == 0 && context->chunk_infos.empty()) {
        _contexts.erase(tablet_id);
        return Status::EndOfFile("done");
    }
    return context->get_chunk_info();
}

void PkIndexLoader::finish_subtask(int64_t tablet_id, const Status& status) {
    std::lock_guard<std::mutex> lg(_mutex);
    auto it = _contexts.find(tablet_id);
    if (it == _contexts.end()) {
        return;
    }
    auto context = it->second;
    context->status = status;
    context->subtask_num--;
    if (!status.is_ok_or_eof() || context->subtask_num == 0) {
        context->cv.notify_all();
    }
}

} // namespace starrocks::lake
