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

#include "storage/tablet_schema_map.h"

#include <bvar/bvar.h>

#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"

namespace starrocks {

static void get_stats(std::ostream& os, void*) {
    const TabletSchemaMap::Stats& stats = GlobalTabletSchemaMap::Instance()->stats();
    os << stats.num_items << "/" << stats.memory_usage;
}

// NOLINTNEXTLINE
bvar::PassiveStatus<std::string> g_schema_map_stats("tablet_schema_map", get_stats, NULL);

bool TabletSchemaMap::check_schema_unique_id(const TabletSchemaPB& schema_pb, const TabletSchemaCSPtr& schema_ptr) {
    if (schema_pb.next_column_unique_id() != schema_ptr->next_column_unique_id() ||
        schema_pb.column_size() != schema_ptr->num_columns()) {
        return false;
    }

    for (size_t i = 0; i < schema_ptr->num_columns(); ++i) {
        int32_t pb_unique_id = schema_pb.column(i).unique_id();
        int32_t unique_id = schema_ptr->column(i).unique_id();
        if (pb_unique_id != unique_id) {
            return false;
        }
    }

    return true;
}

bool TabletSchemaMap::check_schema_unique_id(const TabletSchemaCSPtr& in_schema, const TabletSchemaCSPtr& ori_schema) {
    if (in_schema->keys_type() != ori_schema->keys_type() || in_schema->num_columns() != ori_schema->num_columns() ||
        in_schema->id() != ori_schema->id() || in_schema->schema_version() != ori_schema->schema_version()) {
        return false;
    }

    for (auto i = 0; i < in_schema->num_columns(); i++) {
        if (in_schema->column(i).unique_id() != ori_schema->column(i).unique_id()) {
            return false;
        }
    }
    return true;
}

std::pair<TabletSchemaMap::TabletSchemaPtr, bool> TabletSchemaMap::emplace(const TabletSchemaPB& schema_pb,
                                                                           int64_t tablet_id) {
    SchemaId id = schema_pb.id();
    DCHECK_NE(TabletSchema::invalid_id(), id);
    MapShard* shard = get_shard(id);
    // |result|: return value
    // |ptr|: the shared_ptr whose id is equal to `id` in map
    // |insert|: insert into map or not
    // We use shared schema to save mem usage, but the premise is that we need to ensure that no two different
    // TabletSchemaPBs have the same id. But we can't guarantee it after schema change so far, so we should
    // check the consistent of tablet schema.
    // If check failed, we will create a new unsafe_tablet_schema_ref as return value, but we must hold the original schema
    // in map until the shard lock is release. If not, we may be deconstruct the original schema which will cause
    // a dead lock(#issue 5646)
    TabletSchemaPtr result = nullptr;
    TabletSchemaPtr ptr = nullptr; // Do not move this definition inside the lock scope, otherwise it will deadlock
    bool insert = false;
    {
        std::unique_lock l(shard->mtx);
        auto it = shard->map.find(id);
        if (it == shard->map.end()) {
            result = TabletSchema::create(schema_pb, this);

            _insert(*shard, id, result, tablet_id);
            insert = true;
        } else {
            for (auto& schema_item : it->second) {
                ptr = schema_item.tablet_schema.lock();
                if (UNLIKELY(!ptr)) {
                    continue;
                } else {
                    if (schema_item.tablet_ids.count(tablet_id) > 0) {
                        result = ptr;
                        break;
                    }

                    if (LIKELY(check_schema_unique_id(schema_pb, ptr))) {
                        result = ptr;
                        schema_item.tablet_ids.insert(tablet_id);
                        break;
                    }
                }
            }

            if (result == nullptr) {
                result = TabletSchema::create(schema_pb, this);

                _insert(*shard, id, result, tablet_id);
                insert = true;
            }
        }
    }

    return std::make_pair(result, insert);
}

void TabletSchemaMap::_insert(MapShard& shard, SchemaId id, const TabletSchemaPtr& tablet_schema, int64_t tablet_id) {
    int64_t mem_usage = tablet_schema->mem_usage() + sizeof(int64_t);
    _stats.num_items++;
    _stats.memory_usage += mem_usage;
    MEM_TRACKER_SAFE_CONSUME(GlobalEnv::GetInstance()->tablet_schema_mem_tracker(), mem_usage);

    Item item{tablet_schema, {tablet_id}, mem_usage};
    auto [iter, inserted] = shard.map.try_emplace(id, SchemaItem{item});
    if (!inserted) { // 如果 id 已经存在，则插入新的 item
        iter->second.push_back(std::move(item));
    }
}

std::pair<TabletSchemaMap::TabletSchemaPtr, bool> TabletSchemaMap::emplace(const TabletSchemaPtr& tablet_schema,
                                                                           int64_t tablet_id) {
    DCHECK(tablet_schema != nullptr);
    SchemaId id = tablet_schema->id();
    DCHECK_NE(id, TabletSchema::invalid_id());
    MapShard* shard = get_shard(id);
    bool insert = false;
    TabletSchemaPtr result = nullptr;
    TabletSchemaPtr ptr = nullptr; // Do not move this definition inside the lock scope, otherwise it will deadlock
    {
        std::unique_lock l(shard->mtx);
        auto it = shard->map.find(id);
        if (it == shard->map.end()) {
            result = tablet_schema;

            _insert(*shard, id, result, tablet_id);
            insert = true;
        } else {
            for (auto& schema_item : it->second) {
                ptr = schema_item.tablet_schema.lock();
                if (UNLIKELY(!ptr)) {
                    continue;
                } else {
                    if (schema_item.tablet_ids.count(tablet_id) > 0) {
                        result = ptr;
                        break;
                    }

                    if (LIKELY(check_schema_unique_id(tablet_schema, ptr))) {
                        result = ptr;
                        schema_item.tablet_ids.insert(tablet_id);
                        break;
                    }
                }
            }

            if (result == nullptr) {
                result = tablet_schema;

                _insert(*shard, id, result, tablet_id);
                insert = true;
            }
        }
    }
    return std::make_pair(result, insert);
}

void TabletSchemaMap::erase(SchemaId id, int64_t tablet_id) {
    MapShard* shard = get_shard(id);
    std::lock_guard l(shard->mtx);
    auto iter = shard->map.find(id);
    if (iter != shard->map.end()) {
        for (auto& schema_it : iter->second) {
            schema_it.tablet_ids.erase(tablet_id);
        }
    }
}

TabletSchemaMap::TabletSchemaPtr TabletSchemaMap::get(SchemaId id, int64_t tablet_id) {
    MapShard* shard = get_shard(id);
    TabletSchemaPtr result = nullptr;
    {
        std::lock_guard l(shard->mtx);
        auto iter = shard->map.find(id);
        if (iter != shard->map.end()) {
            for (auto& schema_it : iter->second) {
                if (schema_it.tablet_ids.count(tablet_id) > 0) {
                    result = schema_it.tablet_schema.lock();
                    break;
                }
            }
        }
    }
    return result;
}

bool TabletSchemaMap::contains(SchemaId id, int64_t tablet_id) const {
    const MapShard* shard = get_shard(id);
    std::lock_guard l(shard->mtx);
    bool result = false;
    auto iter = shard->map.find(id);
    if (iter != shard->map.end()) {
        for (auto& shema_it : iter->second) {
            if (shema_it.tablet_ids.count(tablet_id) > 0) {
                result = true;
                break;
            }
        }
    }
    return result;
}

const TabletSchemaMap::Stats& TabletSchemaMap::stats() const {
    return _stats;
}

void TabletSchemaMap::trash_sweep() {
    int64_t timeout_ms = config::tablet_schema_map_trash_sweep_execute_ms;
    int64_t t_start = MonotonicMillis();

    {
        for (auto i = 0; i < kShardSize; ++i) {
            MapShard* shard = &_map_shards[_trash_shard_idx];
            std::lock_guard l(shard->mtx);

            int32_t total_num = shard->map.size();
            if (total_num == 0) {
                _trash_shard_idx = (_trash_shard_idx + 1) % kShardSize;
                continue;
            }

            int32_t scan_num = _trash_shard_off[_trash_shard_idx] % total_num;
            auto it = shard->map.begin();
            std::advance(it, scan_num);

            while (it != shard->map.end()) {
                for (auto schema_it = it->second.begin(); schema_it != it->second.end();) {
                    if (schema_it->tablet_schema.expired()) {
                        _stats.num_items--;
                        _stats.memory_usage -= schema_it->mem_usage;
                        schema_it = it->second.erase(schema_it);
                    } else {
                        ++schema_it;
                    }
                }

                if (it->second.empty()) {
                    it = shard->map.erase(it);
                } else {
                    ++it;
                }

                scan_num++;
                if (MonotonicMillis() - t_start > timeout_ms) {
                    if (scan_num >= total_num) {
                        _trash_shard_off[_trash_shard_idx] = 0;
                        _trash_shard_idx = (_trash_shard_idx + 1) % kShardSize;
                    } else {
                        _trash_shard_off[_trash_shard_idx] = scan_num;
                    }
                    return;
                }
            }

            _trash_shard_off[_trash_shard_idx] = 0;
            _trash_shard_idx = (_trash_shard_idx + 1) % kShardSize;
        }
    }
}

} // namespace starrocks
