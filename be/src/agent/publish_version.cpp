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

#include "publish_version.h"

#include <bvar/bvar.h>

#include "common/compiler_util.h"
#include "common/tracer.h"
#include "fmt/format.h"
#include "gen_cpp/AgentService_types.h"
#include "gutil/strings/join.h"
#include "storage/data_dir.h"
#include "storage/replication_txn_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_manager.h"
#include "storage/txn_manager.h"
#include "util/starrocks_metrics.h"
#include "util/threadpool.h"
#include "util/time.h"

namespace starrocks {

const uint32_t PUBLISH_VERSION_SUBMIT_MAX_RETRY = 10;
bvar::LatencyRecorder g_publish_latency("be", "publish");

struct TabletPublishVersionTask {
    // input params
    int64_t txn_id{0};
    int64_t partition_id{0};
    int64_t tablet_id{0};
    int64_t version{0}; // requested publish version
    RowsetSharedPtr rowset;
    // output params
    Status st;
    // max continuous version after publish is done
    // or 0 which means tablet not found or publish task cannot be submitted
    int64_t max_continuous_version{0};
};

void run_publish_version_task(ThreadPoolToken* token, const TPublishVersionRequest& publish_version_req,
                              TFinishTaskRequest& finish_task, std::unordered_set<DataDir*>& affected_dirs,
                              uint32_t wait_time) {
    int64_t start_ts = MonotonicMillis();
    int64_t transaction_id = publish_version_req.transaction_id;

    Span span = Tracer::Instance().start_trace_or_add_span("run_publish_version_task",
                                                           publish_version_req.txn_trace_parent);
    span->SetAttribute("txn_id", transaction_id);
    auto scoped = trace::Scope(span);

    bool enable_sync_publish = publish_version_req.enable_sync_publish;
    std::vector<TabletPublishVersionTask> tablet_tasks;
    size_t num_partition = publish_version_req.partition_version_infos.size();
    size_t num_active_tablet = 0;
    bool is_replication_txn =
            publish_version_req.__isset.txn_type && publish_version_req.txn_type == TTxnType::TXN_REPLICATION;
    if (is_replication_txn) {
        std::vector<std::vector<TTabletId>> partitions(num_partition);
        for (size_t i = 0; i < publish_version_req.partition_version_infos.size(); i++) {
            StorageEngine::instance()->replication_txn_manager()->get_txn_related_tablets(
                    transaction_id, publish_version_req.partition_version_infos[i].partition_id, &partitions[i]);
            num_active_tablet += partitions[i].size();
        }

        tablet_tasks.resize(num_active_tablet);
        size_t tablet_idx = 0;

        for (size_t i = 0; i < publish_version_req.partition_version_infos.size(); i++) {
            for (const auto tablet_id : partitions[i]) {
                auto& task = tablet_tasks[tablet_idx++];
                task.txn_id = transaction_id;
                task.partition_id = publish_version_req.partition_version_infos[i].partition_id;
                task.tablet_id = tablet_id;
                task.version = publish_version_req.partition_version_infos[i].version;
            }
        }
    } else {
        std::vector<std::map<TabletInfo, RowsetSharedPtr>> partitions(num_partition);
        for (size_t i = 0; i < publish_version_req.partition_version_infos.size(); i++) {
            StorageEngine::instance()->txn_manager()->get_txn_related_tablets(
                    transaction_id, publish_version_req.partition_version_infos[i].partition_id, &partitions[i]);
            num_active_tablet += partitions[i].size();
        }

        tablet_tasks.resize(num_active_tablet);
        size_t tablet_idx = 0;

        for (size_t i = 0; i < publish_version_req.partition_version_infos.size(); i++) {
            for (const auto& itr : partitions[i]) {
                auto& task = tablet_tasks[tablet_idx++];
                task.txn_id = transaction_id;
                task.partition_id = publish_version_req.partition_version_infos[i].partition_id;
                task.tablet_id = itr.first.tablet_id;
                task.version = publish_version_req.partition_version_infos[i].version;
                task.rowset = std::move(itr.second);
            }
        }
    }
    span->SetAttribute("num_partition", num_partition);
    span->SetAttribute("num_tablet", num_active_tablet);

    std::mutex affected_dirs_lock;
    for (auto& tablet_task : tablet_tasks) {
        uint32_t retry_time = 0;
        Status st;
        while (retry_time++ < PUBLISH_VERSION_SUBMIT_MAX_RETRY) {
            st = token->submit_func([&]() {
                auto& task = tablet_task;
                auto tablet_span = Tracer::Instance().add_span("tablet_publish_txn", span);
                auto scoped_tablet_span = trace::Scope(tablet_span);
                tablet_span->SetAttribute("txn_id", transaction_id);
                tablet_span->SetAttribute("tablet_id", task.tablet_id);
                tablet_span->SetAttribute("version", task.version);
                if (!task.rowset) {
                    task.st = Status::NotFound(
                            fmt::format("rowset not found  of tablet: {}, txn_id: {}", task.tablet_id, task.txn_id));
                    LOG(WARNING) << task.st;
                    return;
                }
                TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(task.tablet_id);
                if (!tablet) {
                    // tablet may get dropped, it's ok to ignore this situation
                    LOG(WARNING) << fmt::format(
                            "publish_version tablet not found tablet_id: {}, version: {} txn_id: {}", task.tablet_id,
                            task.version, task.txn_id);
                    return;
                }
                {
                    std::lock_guard lg(affected_dirs_lock);
                    affected_dirs.insert(tablet->data_dir());
                }
                if (is_replication_txn) {
                    task.st = StorageEngine::instance()->replication_txn_manager()->publish_txn(
                            task.txn_id, task.partition_id, tablet, task.version);
                    if (!task.st.ok()) {
                        LOG(WARNING) << "Publish txn failed tablet:" << tablet->tablet_id()
                                     << " version:" << task.version << " partition:" << task.partition_id
                                     << " txn_id: " << task.txn_id;
                        std::string_view msg = task.st.message();
                        tablet_span->SetStatus(trace::StatusCode::kError, {msg.data(), msg.size()});
                    } else {
                        LOG(INFO) << "Publish txn success tablet:" << tablet->tablet_id() << " version:" << task.version
                                  << " tablet_max_version:" << tablet->max_continuous_version()
                                  << " partition:" << task.partition_id << " txn_id: " << task.txn_id;
                    }
                } else {
                    task.st = StorageEngine::instance()->txn_manager()->publish_txn(
                            task.partition_id, tablet, task.txn_id, task.version, task.rowset, wait_time);
                    if (!task.st.ok()) {
                        LOG(WARNING) << "Publish txn failed tablet:" << tablet->tablet_id()
                                     << " version:" << task.version << " partition:" << task.partition_id
                                     << " txn_id: " << task.txn_id << " rowset:" << task.rowset->rowset_id();
                        std::string_view msg = task.st.message();
                        tablet_span->SetStatus(trace::StatusCode::kError, {msg.data(), msg.size()});
                    } else {
                        LOG(INFO) << "Publish txn success tablet:" << tablet->tablet_id() << " version:" << task.version
                                  << " tablet_max_version:" << tablet->max_continuous_version()
                                  << " partition:" << task.partition_id << " txn_id: " << task.txn_id
                                  << " rowset:" << task.rowset->rowset_id();
                    }
                }
            });
            if (st.is_service_unavailable()) {
                int64_t retry_sleep_ms = 50 * retry_time;
                LOG(WARNING) << "publish version threadpool is busy, retry in  " << retry_sleep_ms
                             << "ms. txn_id: " << transaction_id << ", tablet:" << tablet_task.tablet_id;
                // In general, publish version is fast. A small sleep is needed here.
                auto wait_span = Tracer::Instance().add_span("retry_wait", span);
                SleepFor(MonoDelta::FromMilliseconds(retry_sleep_ms));
                continue;
            } else {
                break;
            }
        }
        if (!st.ok()) {
            tablet_task.st = std::move(st);
        }
    }
    span->AddEvent("all_task_submitted");
    token->wait();
    span->AddEvent("all_task_finished");

    Status st;
    finish_task.__isset.tablet_versions = true;
    auto& error_tablet_ids = finish_task.error_tablet_ids;
    auto& tablet_versions = finish_task.tablet_versions;
    auto& tablet_publish_versions = finish_task.tablet_publish_versions;
    tablet_versions.reserve(tablet_tasks.size());
    for (auto& task : tablet_tasks) {
        if (!task.st.ok()) {
            error_tablet_ids.push_back(task.tablet_id);
            if (st.ok()) {
                st = task.st;
            }
        } else {
            auto& pair = tablet_publish_versions.emplace_back();
            pair.__set_tablet_id(task.tablet_id);
            pair.__set_version(task.version);
        }
    }
    // return tablet and its version which has already finished.
    int total_tablet_cnt = 0;
    for (const auto& partition_version : publish_version_req.partition_version_infos) {
        std::vector<TabletInfo> tablet_infos;
        StorageEngine::instance()->tablet_manager()->get_tablets_by_partition(partition_version.partition_id,
                                                                              tablet_infos);
        total_tablet_cnt += tablet_infos.size();
        for (const auto& tablet_info : tablet_infos) {
            TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_info.tablet_id);
            if (!tablet) {
                // tablet may get dropped, it's ok to ignore this situation
                LOG(WARNING) << fmt::format("publish_version tablet not found tablet_id: {}, version: {} txn_id: {}",
                                            tablet_info.tablet_id, partition_version.version, transaction_id);
            } else {
                const int64_t max_continuous_version =
                        enable_sync_publish ? tablet->max_continuous_version() : tablet->max_readable_version();
                if (max_continuous_version > 0) {
                    auto& pair = tablet_versions.emplace_back();
                    pair.__set_tablet_id(tablet_info.tablet_id);
                    pair.__set_version(max_continuous_version);
                }
            }
        }
    }

    // TODO: add more information to status, rather than just first error tablet
    st.to_thrift(&finish_task.task_status);
    if (!error_tablet_ids.empty()) {
        finish_task.__isset.error_tablet_ids = true;
    }

    auto publish_latency = MonotonicMillis() - start_ts;
    g_publish_latency << publish_latency;
    if (st.ok()) {
        if (is_replication_txn) {
            (void)ExecEnv::GetInstance()->delete_file_thread_pool()->submit_func([transaction_id]() {
                StorageEngine::instance()->replication_txn_manager()->clear_txn(transaction_id);
            });
        }
        LOG(INFO) << "publish_version success. txn_id: " << transaction_id << " #partition:" << num_partition
                  << " #tablet:" << tablet_tasks.size() << " time:" << publish_latency << "ms"
                  << " #already_finished:" << total_tablet_cnt - num_active_tablet;
    } else {
        StarRocksMetrics::instance()->publish_task_failed_total.increment(1);
        LOG(WARNING) << "publish_version has error. txn_id: " << transaction_id << " #partition:" << num_partition
                     << " #tablet:" << tablet_tasks.size() << " error_tablets(" << error_tablet_ids.size()
                     << "):" << JoinInts(error_tablet_ids, ",") << " time:" << publish_latency << "ms"
                     << " #already_finished:" << total_tablet_cnt - num_active_tablet;
    }
}

} // namespace starrocks
