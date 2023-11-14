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

#include "storage/chunk_helper.h"
#include "storage/lake/lake_primary_index.h"
#include "storage/lake/update_manager.h"
#include "storage/olap_common.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/segment_options.h"
#include "util/defer_op.h"

namespace starrocks::lake {

class LoadPkIndexSubtask : public Runnable {
public:
    LoadPkIndexSubtask(Tablet* tablet, uint32_t rowset_id, uint32_t seg_id, std::string seg_name, const Schema& schema,
                       int64_t version, const MetaFileBuilder* builder, LakePrimaryIndex* index)
            : _tablet(tablet),
              _rowset_id(rowset_id),
              _seg_id(seg_id),
              _seg_name(std::move(seg_name)),
              _schema(std::move(schema)),
              _version(version),
              _builder(builder),
              _index(index) {}

    void run() override {
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
        vector<uint32_t> rowids;
        rowids.reserve(4096);
        auto chunk_shared_ptr = ChunkHelper::new_chunk(_schema, 4096);
        auto chunk = chunk_shared_ptr.get();
        std::unique_ptr<Column> pk_column;
        if (_schema.num_key_fields() > 1) {
            // more than one key column
            if (!PrimaryKeyEncoder::create_column(_schema, &pk_column).ok()) {
                CHECK(false) << "create column for primary key encoder failed";
            }
        }
        while (true) {
            chunk->reset();
            rowids.clear();
            st = (*seg_itr)->get_next(chunk, &rowids);
            if (st.is_end_of_file()) {
                break;
            } else if (!st.ok()) {
                return;
            } else {
                Column* pkc = nullptr;
                if (pk_column) {
                    pk_column->reset_column();
                    PrimaryKeyEncoder::encode(_schema, *chunk, 0, chunk->num_rows(), pk_column.get());
                    pkc = pk_column.get();
                } else {
                    pkc = chunk->columns()[0].get();
                }
                st = _index->insert(_rowset_id + _seg_id, rowids, *pkc);
                if (!st.ok()) {
                    return;
                }
            }
        }
        (*seg_itr)->close();
    }

private:
    Tablet* _tablet;
    uint32_t _rowset_id;
    uint32_t _seg_id;
    std::string _seg_name;
    const Schema _schema;
    int64_t _version;
    const MetaFileBuilder* _builder;
    LakePrimaryIndex* _index;
};

Status PkIndexLoader::init() {
    return ThreadPoolBuilder("lake_load_pk_index")
            .set_max_threads(config::lake_pk_index_loader_thread)
            .build(&_load_thread_pool);
}

PkIndexLoader::~PkIndexLoader() {
    if (_load_thread_pool) {
        _load_thread_pool->shutdown();
    }
}

std::future<Status> PkIndexLoader::load(Tablet* tablet, const std::vector<RowsetPtr>& rowsets, const Schema& schema,
                                        int64_t version, const MetaFileBuilder* builder, LakePrimaryIndex* index) {
    uint64_t subtask_num = 0;
    auto p = std::make_unique<std::promise<Status>>();
    std::future<Status> f = p->get_future();
    std::vector<std::shared_ptr<Runnable>> subtasks;
    for (auto& rowset : rowsets) {
        auto& rowset_meta = rowset->metadata();
        for (uint32_t i = 0; i < rowset_meta.segments_size(); ++i) {
            std::shared_ptr<Runnable> subtask(std::make_shared<LoadPkIndexSubtask>(
                    tablet, rowset->id(), i, rowset_meta.segments(i), schema, version, builder, index));
            subtasks.push_back(subtask);
        }
        subtask_num += rowset_meta.segments_size();
    }
    if (subtask_num == 0) {
        p->set_value(Status::OK());
        return f;
    }

    std::lock_guard<std::mutex> lg(_mutex);
    for (auto& subtask : subtasks) {
        auto st = _load_thread_pool->submit(std::move(subtask));
        if (!st.ok()) {
            LOG(ERROR) << "Fail to submit pk index loading subtask, error: " << st.to_string();
            subtask_num--;
        }
    }
    _subtask_nums.emplace(tablet->id(), subtask_num);
    _promises.emplace(tablet->id(), std::move(p));
    return f;
}

void PkIndexLoader::finish_subtask(int64_t tablet_id, const Status& status) {
    std::lock_guard<std::mutex> lg(_mutex);
    auto it = _subtask_nums.find(tablet_id);
    if (it == _subtask_nums.end()) {
        return;
    }
    it->second--;
    if (!status.ok() || it->second == 0) {
        _promises[tablet_id]->set_value(status);
        _subtask_nums.erase(tablet_id);
        _promises.erase(tablet_id);
        return;
    }
}

} // namespace starrocks::lake
