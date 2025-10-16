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

#ifdef USE_STAROS

#include "storage/lake/tablet_cache_stats_manager.h"

#include "storage/lake/tablet_manager.h"
#include "util/starrocks_metrics.h"
#include "util/threadpool.h"

namespace starrocks::lake {

TabletCacheStatsManager::TabletCacheStatsManager(TabletManager* tablet_mgr) : _tablet_mgr(tablet_mgr) {}

TabletCacheStatsManager::~TabletCacheStatsManager() {
    if (!_stopped) {
        stop();
    }
}

void TabletCacheStatsManager::init() {
    int max_threads = config::tablet_cache_stats_max_threads;
    auto st = ThreadPoolBuilder("tablet_cache_stats")
                      .set_min_threads(1)
                      .set_max_threads(max_threads)
                      .set_max_queue_size(INT_MAX)
                      .build(&_thread_pool);
    CHECK(st.ok()) << st;
    REGISTER_THREAD_POOL_METRICS(tablet_cache_stats, _thread_pool);
    _stopped = false;
}

void TabletCacheStatsManager::stop() {
    bool expected = false;
    if (_stopped.compare_exchange_strong(expected, true)) {
        _thread_pool->shutdown();
    }
}

Status TabletCacheStatsManager::update_max_threads(int max_threads) {
    if (_thread_pool != nullptr) {
        return _thread_pool->update_max_threads(max_threads);
    } else {
        return Status::InternalError("Thread pool not exist");
    }
}

std::shared_ptr<TabletCacheStatsContext> TabletCacheStatsManager::submit_get_tablet_cache_stats(
        int64_t tablet_id, int64_t tablet_version) {
    auto ctx = std::make_shared<TabletCacheStatsContext>(tablet_id, tablet_version);
    if (_stopped) {
        ctx->fail(Status::Aborted("tablet cache stats manager stopped!"));
        return ctx;
    }
    auto st = _thread_pool->submit_func(std::bind(&TabletCacheStatsManager::_get_tablet_cache_stats, this, ctx));
    if (!st.ok()) {
        ctx->fail(st);
        return ctx;
    }
    return ctx;
}

std::shared_ptr<TabletCacheStatsContext> TabletCacheStatsManager::get_tablet_cache_stats(int64_t tablet_id,
                                                                                         int64_t tablet_version) {
    auto ctx = std::make_shared<TabletCacheStatsContext>(tablet_id, tablet_version);
    if (_stopped) {
        ctx->fail(Status::Aborted("tablet cache stats manager stopped!"));
        return ctx;
    }
    _get_tablet_cache_stats(ctx);
    return ctx;
}

void TabletCacheStatsManager::_get_tablet_cache_stats(std::shared_ptr<TabletCacheStatsContext>& ctx) {
    int64_t tablet_id = ctx->tablet_id;
    int64_t tablet_version = ctx->tablet_version;
    auto tablet_metadata = _tablet_mgr->get_tablet_metadata(tablet_id, tablet_version, false /* fill_cache */);
    if (!tablet_metadata.ok()) {
        ctx->fail(Status::Aborted(fmt::format("fail to get tablet meta for tablet {}, version {}, error: {}", tablet_id,
                                              tablet_version, tablet_metadata.status().message())));
        return;
    }

    const auto& metadata = *tablet_metadata;
    int64_t cached_bytes = 0;
    int64_t total_bytes = 0;
    for (const auto& rowset : metadata->rowsets()) {
        const auto& segment_cnt = rowset.segments_size();
        bool has_segment_size = (segment_cnt == rowset.segment_size_size());
        bool is_bundled_file = (segment_cnt == rowset.bundle_file_offsets_size());
        for (size_t i = 0; i < segment_cnt; ++i) {
            std::string segment_path = _tablet_mgr->segment_location(tablet_id, rowset.segments().Get(i));
            auto fs_or = FileSystem::CreateSharedFromString(segment_path);
            if (!fs_or.ok()) {
                ctx->fail(fs_or.status());
                return;
            }
            auto result = (*fs_or)->get_cache_stats(
                    segment_path, is_bundled_file ? rowset.bundle_file_offsets().Get(i) : 0 /* offset */,
                    has_segment_size ? rowset.segment_size().Get(i) : -1 /* size */);
            if (!result.ok()) {
                ctx->fail(result.status());
                return;
            } else {
                cached_bytes += (*result).first;
                total_bytes += (*result).second;
            }
        }
    }

    for (const auto& [_, file] : metadata->delvec_meta().version_to_file()) {
        std::string delvec_path = _tablet_mgr->delvec_location(tablet_id, file.name());
        auto fs_or = FileSystem::CreateSharedFromString(delvec_path);
        if (!fs_or.ok()) {
            ctx->fail(fs_or.status());
            return;
        }
        auto result = (*fs_or)->get_cache_stats(delvec_path, 0 /* offset */, file.size());
        if (!result.ok()) {
            ctx->fail(result.status());
            return;
        } else {
            cached_bytes += (*result).first;
            total_bytes += (*result).second;
        }
    }

    ctx->done(cached_bytes, total_bytes);
    VLOG(3) << "finished get cache stats for tablet: " << tablet_id << ", version: " << tablet_version
            << ", cached_bytes: " << cached_bytes << ", total_bytes: " << total_bytes;
}

} // namespace starrocks::lake

#endif
