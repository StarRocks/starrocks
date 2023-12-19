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

#include <chrono>

#include "common/constexpr.h"
#include "exec/workgroup/scan_executor.h"
#include "exec/workgroup/work_group.h"
#include "storage/chunk_helper.h"
#include "storage/lake/tablet_manager.h"
#include "storage/lake/update_manager.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment_options.h"
#include "util/defer_op.h"

namespace starrocks::lake {

class PkSegmentScanTask {
public:
    PkSegmentScanTask(TabletManager* tablet_mgr, TabletMetadataPtr metadata, uint32_t rowset_id, uint32_t seg_id,
                      std::string seg_name, Schema schema, int64_t version, const MetaFileBuilder* builder)
            : _tablet_mgr(tablet_mgr),
              _metadata(std::move(metadata)),
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
            ExecEnv::GetInstance()->lake_pk_index_loader()->finish_subtask(_metadata->id(), st);
        });
        size_t footer_size_hint = 16 * 1024;
        std::shared_ptr<TabletSchema> tablet_schema = std::make_shared<TabletSchema>(_metadata->schema());
        auto segment_path = _tablet_mgr->segment_location(_metadata->id(), _seg_name);
        auto segment_info = FileInfo{.path = segment_path};
        LakeIOOptions lake_io_opts{.fill_data_cache = false};
        auto seg =
                _tablet_mgr->load_segment(segment_info, _seg_id, &footer_size_hint, lake_io_opts, false, tablet_schema);
        if (!seg.ok()) {
            st = seg.status();
            return;
        }
        SegmentReadOptions seg_options;
        auto fs = FileSystem::CreateSharedFromString(_tablet_mgr->tablet_root_location(_metadata->id()));
        if (!fs.ok()) {
            st = fs.status();
            return;
        }
        OlapReaderStatistics stats;
        seg_options.fs = *fs;
        seg_options.stats = &stats;
        seg_options.is_primary_keys = true;
        seg_options.delvec_loader = std::make_shared<LakeDelvecLoader>(_tablet_mgr->update_mgr(), _builder);
        seg_options.version = _version;
        seg_options.tablet_id = _metadata->id();
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
            ExecEnv::GetInstance()->lake_pk_index_loader()->add_chunk(_metadata->id(), chunk_info);
        }
        (*seg_itr)->close();
    }

private:
    TabletManager* _tablet_mgr;
    const TabletMetadataPtr _metadata;
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

Status PkIndexLoader::load(TabletManager* tablet_mgr, const TabletMetadataPtr& metadata,
                           const std::vector<RowsetPtr>& rowsets, const Schema& schema, int64_t version,
                           const MetaFileBuilder* builder) {
    if (_contexts.count(metadata->id())) {
        auto error_msg = fmt::format("Already exists pk segment scan substask, tablet: {}", metadata->id());
        LOG(ERROR) << error_msg;
        return Status::AlreadyExist(error_msg);
    }

    uint64_t subtask_num = 0;
    std::vector<std::shared_ptr<PkSegmentScanTask>> scan_tasks;
    for (auto& rowset : rowsets) {
        auto& rowset_meta = rowset->metadata();
        for (uint32_t i = 0; i < rowset_meta.segments_size(); ++i) {
            auto scan_task = std::make_shared<PkSegmentScanTask>(tablet_mgr, metadata, rowset->id(), i,
                                                                 std::move(rowset_meta.segments(i)), std::move(schema),
                                                                 version, builder);
            bool ret = ExecEnv::GetInstance()->connector_scan_executor()->submit(
                    workgroup::ScanTask(workgroup::WorkGroupManager::instance()->get_default_workgroup().get(),
                                        [scan_task](workgroup::YieldContext&) { scan_task->run(); }));
            if (!ret) {
                auto error_msg = fmt::format("Fail to submit pk segment scan subtask, tablet: {}", metadata->id());
                LOG(ERROR) << error_msg;
                return Status::IOError(error_msg);
            }
        }
        subtask_num += rowset_meta.segments_size();
    }

    std::lock_guard<std::mutex> lg(_mutex);
    auto context = std::make_shared<PkSegmentScanTaskContext>();
    context->subtask_num = subtask_num;
    _contexts.emplace(metadata->id(), context);
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

    context->cv.wait_for(lk, std::chrono::seconds(1), [&] {
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
