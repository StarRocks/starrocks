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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/txn_manager.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "txn_manager.h"

#include <fmt/format.h>

#include <algorithm>
#include <new>
#include <queue>
#include <set>

#include "common/tracer.h"
#include "exec/schema_scanner/schema_be_txns_scanner.h"
#include "storage/data_dir.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta.h"
#include "util/runtime_profile.h"
#include "util/scoped_cleanup.h"
#include "util/starrocks_metrics.h"
#include "util/threadpool.h"
#include "util/time.h"

namespace starrocks {

std::string name(TxnState txn_state) {
    if (txn_state >= 0 && txn_state < 4) {
        return TxnStateNames[txn_state];
    }
    return "INVALID";
}

// keep recently published txns in memory for a while
static std::mutex txn_info_history_lock;
static std::deque<TxnInfo> txn_info_history;

static void add_txn_info_history(TxnInfo& info) {
    std::lock_guard<std::mutex> l(txn_info_history_lock);
    txn_info_history.push_back(info);
    while (txn_info_history.size() > config::txn_info_history_size) {
        txn_info_history.pop_front();
    }
}

static void to_txn_info(int64_t txn_id, int64_t partition_id,
                        const std::pair<TabletInfo, TabletTxnInfo>& tablet_load_it, TxnInfo& info) {
    info.load_id = tablet_load_it.second.load_id;
    info.txn_id = txn_id;
    info.partition_id = partition_id;
    info.tablet_id = tablet_load_it.first.tablet_id;
    info.create_time = tablet_load_it.second.creation_time;
    info.commit_time = tablet_load_it.second.commit_time;
    if (tablet_load_it.second.rowset != nullptr) {
        auto& rowset = tablet_load_it.second.rowset;
        info.rowset_id = rowset->rowset_id().to_string();
        info.num_segment = rowset->num_segments();
        info.num_delfile = rowset->num_delete_files();
        info.num_row = rowset->num_rows();
        info.data_size = rowset->data_disk_size();
    }
}

void TxnManager::get_txn_infos(int64_t txn_id, int64_t tablet_id, std::vector<TxnInfo>& txn_infos) {
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        std::shared_lock txn_rdlock(_txn_map_locks[i]);
        for (auto& it : _txn_tablet_maps[i]) {
            auto& txn_key = it.first;
            if (txn_id != -1 && txn_key.second != txn_id) {
                continue;
            }
            for (auto& tablet_load_it : it.second) {
                if (tablet_id != -1 && tablet_load_it.first.tablet_id != tablet_id) {
                    continue;
                }
                auto& info = txn_infos.emplace_back();
                to_txn_info(txn_key.second, txn_key.first, tablet_load_it, info);
            }
        }
    }
    std::lock_guard lg(txn_info_history_lock);
    for (auto& info : txn_info_history) {
        if (txn_id != -1 && info.txn_id != txn_id) {
            continue;
        }
        if (tablet_id != -1 && info.tablet_id != tablet_id) {
            continue;
        }
        txn_infos.push_back(info);
    }
    LOG(INFO) << "get txn infos, txn_id=" << txn_id << ", tablet_id=" << tablet_id
              << ", txn_infos.size=" << txn_infos.size();
}

TxnManager::TxnManager(int32_t txn_map_shard_size, int32_t txn_shard_size, uint32_t store_num)
        : _txn_map_shard_size(txn_map_shard_size), _txn_shard_size(txn_shard_size) {
    DCHECK_GT(_txn_map_shard_size, 0);
    DCHECK_GT(_txn_shard_size, 0);
    DCHECK_EQ(_txn_map_shard_size & (_txn_map_shard_size - 1), 0);
    DCHECK_EQ(_txn_shard_size & (_txn_shard_size - 1), 0);
    _txn_map_locks = std::unique_ptr<std::shared_mutex[]>(new std::shared_mutex[_txn_map_shard_size]);
    _txn_tablet_maps = std::unique_ptr<txn_tablet_map_t[]>(new txn_tablet_map_t[_txn_map_shard_size]);
    _txn_partition_maps = std::unique_ptr<txn_partition_map_t[]>(new txn_partition_map_t[_txn_map_shard_size]);
    _create_table_txn_map = std::unique_ptr<create_table_txn_map_t>(new create_table_txn_map_t());
    _tablet_in_txn = std::unique_ptr<std::set<int64_t>>(new std::set<int64_t>());
    // we will get "store_num = 0" if it acts as cn, just ignore flush pool
    if (store_num > 0) {
        auto st = ThreadPoolBuilder("meta-flush")
                          .set_min_threads(1)
                          .set_max_threads(store_num * 2)
                          .set_idle_timeout(MonoDelta::FromSeconds(30))
                          .build(&_flush_thread_pool);
        CHECK(st.ok());
    }
}

Status TxnManager::prepare_txn(TPartitionId partition_id, const TabletSharedPtr& tablet, TTransactionId transaction_id,
                               const PUniqueId& load_id) {
    auto scoped =
            trace::Scope(Tracer::Instance().start_trace_txn_tablet("txn_prepare", transaction_id, tablet->tablet_id()));
    return prepare_txn(partition_id, transaction_id, tablet->tablet_id(), tablet->schema_hash(), tablet->tablet_uid(),
                       load_id);
}

Status TxnManager::prepare_create_txn(TTransactionId txn_id, const CreateTableTxn& create_table_txn) {
    std::unique_lock wr_lock(_create_table_lock);
    auto it = _create_table_txn_map->find(txn_id);
    if (it == _create_table_txn_map->end()) {
        _create_table_txn_map->emplace(txn_id, create_table_txn);
        for (auto tablet_id : create_table_txn.tablet_ids) {
            _tablet_in_txn->emplace(tablet_id);
        }
        return Status::OK();
    }
    if (create_table_txn.txn_state == TxnState::TXN_PREPARED || create_table_txn.txn_state == TXN_COMMITTED) {
        return Status::AlreadyExist(fmt::format("transaction already exist. txn_id: {}", txn_id));
    } else {
        return Status::Aborted(fmt::format("transaction aborted. txn_id: {}", txn_id));
    }
}

Status TxnManager::commit_txn(TPartitionId partition_id, const TabletSharedPtr& tablet, TTransactionId transaction_id,
                              const PUniqueId& load_id, const RowsetSharedPtr& rowset_ptr, bool is_recovery) {
    auto scoped =
            trace::Scope(Tracer::Instance().start_trace_txn_tablet("txn_commit", transaction_id, tablet->tablet_id()));
    return commit_txn(tablet->data_dir()->get_meta(), partition_id, transaction_id, tablet->tablet_id(),
                      tablet->schema_hash(), tablet->tablet_uid(), load_id, rowset_ptr, is_recovery);
}

Status TxnManager::commit_create_txn(TTransactionId txn_id, const CreateTableTxn& create_table_txn) {
    std::unique_lock wr_lock(_create_table_lock);
    auto it = _create_table_txn_map->find(txn_id);
    if (it == _create_table_txn_map->end()) {
        return Status::NotFound(fmt::format("Not found txn. txn_id: {}", txn_id));
    }
    if (it->second.txn_state == TxnState::TXN_PREPARED) {
        _create_table_txn_map->insert_or_assign(txn_id, create_table_txn);
    } else if (it->second.txn_state == TxnState::TXN_COMMITTED) {
        return Status::AlreadyExist(fmt::format("transaction already exist. txn_id: {}", txn_id));
    } else if (it->second.txn_state == TxnState::TXN_ABORTED) {
        return Status::Aborted(fmt::format("transaction aborted. txn_id: {}", txn_id));
    }
    return Status::OK();
}

// delete the txn from manager if it is not committed(not have a valid rowset)
Status TxnManager::rollback_txn(TPartitionId partition_id, const TabletSharedPtr& tablet, TTransactionId transaction_id,
                                bool with_log) {
    auto scoped = trace::Scope(
            Tracer::Instance().start_trace_txn_tablet("txn_rollback", transaction_id, tablet->tablet_id()));
    return rollback_txn(partition_id, transaction_id, tablet->tablet_id(), tablet->schema_hash(), tablet->tablet_uid(),
                        with_log);
}

Status TxnManager::delete_txn(TPartitionId partition_id, const TabletSharedPtr& tablet, TTransactionId transaction_id) {
    return delete_txn(tablet->data_dir()->get_meta(), partition_id, transaction_id, tablet->tablet_id(),
                      tablet->schema_hash(), tablet->tablet_uid());
}

Status TxnManager::add_create_txn(TTransactionId txn_id, const CreateTableTxn& create_table_txn) {
    std::unique_lock wr_lock(_create_table_lock);
    _create_table_txn_map->emplace(txn_id, create_table_txn);
    for (auto tablet_id : create_table_txn.tablet_ids) {
        _tablet_in_txn->emplace(tablet_id);
    }
    return Status::OK();
}

Status TxnManager::delete_create_txn(TTransactionId txn_id, const CreateTableTxn& create_table_txn) {
    std::unique_lock wr_lock(_create_table_lock);
    _create_table_txn_map->erase(txn_id);
    for (auto tablet_id : create_table_txn.tablet_ids) {
        _tablet_in_txn->erase(tablet_id);
    }
    return Status::OK();
}

Status TxnManager::delete_create_txn(const std::vector<TTransactionId>& txn_ids,
                                     const std::vector<CreateTableTxn>& create_table_txns) {
    std::unique_lock wr_lock(_create_table_lock);
    for (int i = 0; i < txn_ids.size(); ++i) {
        const int64_t& txn_id = txn_ids[i];
        _create_table_txn_map->erase(txn_id);
        const CreateTableTxn& create_table_txn = create_table_txns[i];
        for (auto tablet_id : create_table_txn.tablet_ids) {
            _tablet_in_txn->erase(tablet_id);
        }
    }
    return Status::OK();
}

StatusOr<CreateTableTxn*> TxnManager::get_create_txn(TTransactionId txn_id) {
    std::unique_lock wr_lock(_create_table_lock);
    auto it = _create_table_txn_map->find(txn_id);
    if (it == _create_table_txn_map->end()) {
        return Status::NotFound(fmt::format("Not found txn. txn_id: {}", txn_id));
    }
    return &(it->second);
}

Status TxnManager::list_unused_create_txn(std::vector<CreateTableTxn>* create_table_txns) {
    std::unique_lock wr_lock(_create_table_lock);
    int64_t now = UnixSeconds();
    for (auto it : *_create_table_txn_map) {
        if (it.second.txn_state == TXN_COMMITTED) {
            continue;
        }
        if (difftime(now, it.second.txn_start_time) > it.second.txn_timeout) {
            create_table_txns->push_back(it.second);
        }
    }
    return Status::OK();
}

bool TxnManager::check_tablet_in_txn(int64_t tablet_id) {
    std::unique_lock wr_lock(_create_table_lock);
    return _tablet_in_txn->find(tablet_id) == _tablet_in_txn->end();
}

// prepare txn should always be allowed because ingest task will be retried
// could not distinguish rollup, schema change or base table, prepare txn successfully will allow
// ingest retried
Status TxnManager::prepare_txn(TPartitionId partition_id, TTransactionId transaction_id, TTabletId tablet_id,
                               SchemaHash schema_hash, const TabletUid& tablet_uid, const PUniqueId& load_id) {
    TxnKey key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    std::unique_lock txn_wrlock(_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it != txn_tablet_map.end()) {
        auto load_itr = it->second.find(tablet_info);
        if (load_itr != it->second.end()) {
            // found load for txn,tablet
            // case 1: user commit rowset, then the load id must be equal
            TabletTxnInfo& load_info = load_itr->second;
            // check if load id is equal
            if (load_info.load_id.hi() == load_id.hi() && load_info.load_id.lo() == load_id.lo() &&
                load_info.rowset != nullptr) {
                LOG(WARNING) << "Transaction already exists. tablet: " << tablet_info.tablet_id
                             << ", partition_id: " << key.first << ", txn_id: " << key.second;
                return Status::OK();
            }
        }
    }

    // check if there are too many transactions on running.
    // if yes, reject the request.
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id);
    if (txn_partition_map.size() > config::max_runnings_transactions_per_txn_map) {
        auto msg = fmt::format("Txn number exceeds the limit. txn_count: {}, limit: {}", txn_tablet_map.size(),
                               config::max_runnings_transactions_per_txn_map);
        LOG(ERROR) << msg;
        return Status::ServiceUnavailable(msg);
    }

    // not found load id
    // case 1: user start a new txn, rowset_ptr = null
    // case 2: loading txn from meta env
    TabletTxnInfo load_info(load_id, nullptr);
    txn_tablet_map[key][tablet_info] = load_info;
    _insert_txn_partition_map_unlocked(transaction_id, partition_id);

    VLOG(3) << "add transaction to engine successfully."
            << "partition_id: " << key.first << ", txn_id: " << key.second << ", tablet: " << tablet_info.to_string();
    return Status::OK();
}

Status TxnManager::commit_txn(KVStore* meta, TPartitionId partition_id, TTransactionId transaction_id,
                              TTabletId tablet_id, SchemaHash schema_hash, const TabletUid& tablet_uid,
                              const PUniqueId& load_id, const RowsetSharedPtr& rowset_ptr, bool is_recovery) {
    if (partition_id < 1 || transaction_id < 1 || tablet_id < 1) {
        LOG(FATAL) << "Invalid commit req "
                   << " partition_id=" << partition_id << " txn_id: " << transaction_id << " tablet_id=" << tablet_id;
    }
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    if (rowset_ptr == nullptr) {
        auto msg = fmt::format("Fail to commit txn. tablet_id: {}, partition_id: {}, txn_Id: {}", tablet_id, key.first,
                               key.second);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }

    {
        // get tx
        std::shared_lock rdlock(_get_txn_map_lock(transaction_id));
        txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
        auto it = txn_tablet_map.find(key);
        if (it != txn_tablet_map.end()) {
            auto load_itr = it->second.find(tablet_info);
            if (load_itr != it->second.end()) {
                // found load for txn,tablet
                // case 1: user commit rowset, then the load id must be equal
                TabletTxnInfo& load_info = load_itr->second;
                // check if load id is equal
                if (load_info.load_id.hi() == load_id.hi() && load_info.load_id.lo() == load_id.lo() &&
                    load_info.rowset != nullptr && load_info.rowset->rowset_id() == rowset_ptr->rowset_id()) {
                    // find a rowset with same rowset id, then it means a duplicate call
                    LOG(INFO) << "Transaction altready exists. tablet: " << tablet_id << ", partition_id: " << key.first
                              << ", txn_id: " << key.second << ", rowset_id: " << load_info.rowset->rowset_id();
                    return Status::OK();
                } else if (load_info.load_id.hi() == load_id.hi() && load_info.load_id.lo() == load_id.lo() &&
                           load_info.rowset != nullptr && load_info.rowset->rowset_id() != rowset_ptr->rowset_id()) {
                    // find a rowset with different rowset id, then it should not happen, just return errors
                    LOG(WARNING) << "Txn are correpsonds to two different rowsets. "
                                 << "txn_id: " << key.second << ", tablet_id: " << tablet_id
                                 << ", first_rowset_id: " << load_info.rowset->rowset_id()
                                 << ", second_rowset_id: " << rowset_ptr->rowset_id();
                    return Status::InternalError(
                            fmt::format("Txn are correpsonds to two different rowsets. tablet_id: {}, txn_id: {}",
                                        tablet_id, key.second));
                }
            }
        }
    }

    // if not in recovery mode, then should persist the meta to meta env
    // save meta need access disk, it maybe very slow, so that it is not in global txn lock
    // it is under a single txn lock
    if (!is_recovery) {
        RowsetMetaPB rowset_meta_pb;
        rowset_ptr->rowset_meta()->get_full_meta_pb(&rowset_meta_pb);
        Status st = RowsetMetaManager::save(meta, tablet_uid, rowset_meta_pb);
        if (!st.ok()) {
            LOG(WARNING) << "Fail to save committed rowset. "
                         << "tablet_id: " << tablet_id << ", txn_id: " << transaction_id
                         << ", rowset_id: " << rowset_ptr->rowset_id();
            return Status::InternalError(
                    fmt::format("Fail to save committed rowset. tablet_id: {}, txn_id: {}", tablet_id, key.second));
        }
    }

    {
        std::unique_lock wrlock(_get_txn_map_lock(transaction_id));
        txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
        auto& tablet_txn_infos = txn_tablet_map[key];
        auto itr = tablet_txn_infos.find(tablet_info);
        if (itr == tablet_txn_infos.end()) {
            TabletTxnInfo info(load_id, rowset_ptr);
            info.commit_time = UnixSeconds();
            tablet_txn_infos[tablet_info] = info;
        } else {
            itr->second.load_id = load_id;
            itr->second.rowset = rowset_ptr;
            itr->second.commit_time = UnixSeconds();
        }
        // [tablet_info] = load_info;
        _insert_txn_partition_map_unlocked(transaction_id, partition_id);
        LOG(INFO) << "Commit txn successfully. "
                  << " tablet: " << tablet_id << ", txn_id: " << key.second << ", rowsetid: " << rowset_ptr->rowset_id()
                  << " #segment:" << rowset_ptr->num_segments() << " #delfile:" << rowset_ptr->num_delete_files()
                  << " #uptfiles:" << rowset_ptr->num_update_files();
    }
    return Status::OK();
}

Status TxnManager::publish_txn(TPartitionId partition_id, const TabletSharedPtr& tablet, TTransactionId transaction_id,
                               int64_t version, const RowsetSharedPtr& rowset, uint32_t wait_time) {
    if (tablet->updates() != nullptr) {
        StarRocksMetrics::instance()->update_rowset_commit_request_total.increment(1);
        auto st = tablet->rowset_commit(version, rowset, wait_time);
        if (!st.ok()) {
            StarRocksMetrics::instance()->update_rowset_commit_request_failed.increment(1);
            return st;
        }
    } else {
        auto st = tablet->add_inc_rowset(rowset, version);
        tablet->remove_in_writing_data_size(transaction_id);
        if (!st.ok() && !st.is_already_exist()) {
            // TODO: rollback saved rowset if error?
            LOG(WARNING) << "fail to add visible rowset to tablet. rowset_id=" << rowset->rowset_id()
                         << ", tablet_id: " << tablet->tablet_id() << ", txn_id: " << transaction_id << ", res=" << st;
            return st;
        }
    }
    std::unique_lock wrlock(_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet->tablet_id(), tablet->schema_hash(), tablet->tablet_uid());
    auto it = txn_tablet_map.find(key);
    if (it != txn_tablet_map.end()) {
        auto tablet_txn_info_itr = it->second.find(tablet_info);
        if (tablet_txn_info_itr != it->second.end()) {
            TxnInfo txn_info;
            to_txn_info(transaction_id, partition_id, *tablet_txn_info_itr, txn_info);
            txn_info.publish_time = UnixSeconds();
            txn_info.version = version;
            add_txn_info_history(txn_info);
            it->second.erase(tablet_txn_info_itr);
            LOG(INFO) << "add txn info history. txn_id: " << transaction_id << ", partition_id: " << partition_id
                      << ", tablet_id: " << tablet->tablet_id() << ", schema_hash: " << tablet->schema_hash()
                      << ", rowset_id: " << rowset->rowset_id() << ", version: " << version;
        }
        if (it->second.empty()) {
            txn_tablet_map.erase(it);
            _clear_txn_partition_map_unlocked(transaction_id, partition_id);
        }
    }
    return Status::OK();
}

Status TxnManager::persist_tablet_related_txns(const std::vector<TabletSharedPtr>& tablets) {
    int64_t duration_ns = 0;
    SCOPED_RAW_TIMER(&duration_ns);

    std::unordered_set<std::string> persisted;
    std::vector<TabletSharedPtr> to_flush_tablet;
    for (auto& tablet : tablets) {
        if (tablet == nullptr) {
            continue;
        }
        auto path = tablet->data_dir()->path();
        // skip persisted meta.
        if (persisted.find(path) != persisted.end()) continue;
        to_flush_tablet.push_back(tablet);
        persisted.insert(path);
    }

    // using BThreadCountDownLatch to make sure it wouldn't block the brpc worker.
    BThreadCountDownLatch bthread_latch(to_flush_tablet.size());
    auto token = _flush_thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    std::vector<std::pair<Status, int64_t>> pair_vec(to_flush_tablet.size());
    int i = 0;
    for (auto& tablet : to_flush_tablet) {
        auto dir = tablet->data_dir();
        auto st = token->submit_func([&pair_vec, &bthread_latch, dir, i]() {
            pair_vec[i].first = dir->get_meta()->flushWAL();
            bthread_latch.count_down();
        });
        if (!st.ok()) {
            pair_vec[i].first = st;
            bthread_latch.count_down();
        }
        pair_vec[i].second = tablet->tablet_id();
        i++;
    }

    bthread_latch.wait();
    for (const auto& pair : pair_vec) {
        auto& st = pair.first;
        if (!st.ok()) {
            LOG(WARNING) << "Failed to persist tablet meta, tablet_id: " << pair.second << " res: " << st;
            return st;
        }
    }

    StarRocksMetrics::instance()->txn_persist_total.increment(1);
    StarRocksMetrics::instance()->txn_persist_duration_us.increment(duration_ns / 1000);
    return Status::OK();
}

void TxnManager::flush_dirs(std::unordered_set<DataDir*>& affected_dirs) {
    int64_t duration_ns = 0;
    SCOPED_RAW_TIMER(&duration_ns);

    int i = 0;
    std::vector<std::pair<Status, std::string>> pair_vec(affected_dirs.size());
    auto token = _flush_thread_pool->new_token(ThreadPool::ExecutionMode::CONCURRENT);
    for (auto dir : affected_dirs) {
        auto st = token->submit_func([&pair_vec, dir, i]() { pair_vec[i].first = dir->get_meta()->flushWAL(); });
        pair_vec[i].second = dir->path();
        if (!st.ok()) {
            pair_vec[i].first = std::move(st);
        }
        i++;
    }

    // wait for all the flush task complete
    token->wait();
    for (const auto& pair : pair_vec) {
        if (!pair.first.ok()) {
            LOG(WARNING) << "failed to flush tablet meta, dir:" << pair.second << " res:" << pair.first;
        }
    }

    StarRocksMetrics::instance()->txn_persist_total.increment(1);
    StarRocksMetrics::instance()->txn_persist_duration_us.increment(duration_ns / 1000);
}

// txn could be rollbacked if it does not have related rowset
// if the txn has related rowset then could not rollback it, because it
// may be committed in another thread and our current thread meets errors when writing to data file
// BE has to wait for fe call clear txn api
Status TxnManager::rollback_txn(TPartitionId partition_id, TTransactionId transaction_id, TTabletId tablet_id,
                                SchemaHash schema_hash, const TabletUid& tablet_uid, bool with_log) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    std::unique_lock wrlock(_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it != txn_tablet_map.end()) {
        auto load_itr = it->second.find(tablet_info);
        if (load_itr != it->second.end()) {
            // found load for txn,tablet
            // case 1: user commit rowset, then the load id must be equal
            TabletTxnInfo& load_info = load_itr->second;
            if (load_info.rowset != nullptr) {
                // if rowset is not null, it means other thread may commit the rowset
                // should not delete txn any more
                return Status::AlreadyExist(
                        fmt::format("Txn already exists. tablet_id: {}, txn_id: {}", tablet_id, transaction_id));
            }
        }
        it->second.erase(tablet_info);
        if (with_log) {
            LOG(INFO) << "rollback transaction from engine successfully."
                      << " partition_id: " << key.first << ", txn_id: " << key.second
                      << ", tablet: " << tablet_info.to_string();
        }
        if (it->second.empty()) {
            txn_tablet_map.erase(it);
            _clear_txn_partition_map_unlocked(transaction_id, partition_id);
        }
    }
    return Status::OK();
}

// fe call this api to clear unused rowsets in be
// could not delete the rowset if it already has a valid version
Status TxnManager::delete_txn(KVStore* meta, TPartitionId partition_id, TTransactionId transaction_id,
                              TTabletId tablet_id, SchemaHash schema_hash, const TabletUid& tablet_uid) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    std::unique_lock txn_wrlock(_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it == txn_tablet_map.end()) {
        return Status::NotFound(fmt::format("Not found txn. tablet_id: {}, txn_id: {}", tablet_id, transaction_id));
    }
    auto load_itr = it->second.find(tablet_info);
    if (load_itr != it->second.end()) {
        // found load for txn,tablet
        // case 1: user commit rowset, then the load id must be equal
        TabletTxnInfo& load_info = load_itr->second;
        if (load_info.rowset != nullptr && meta != nullptr) {
            if (load_info.rowset->version().first > 0) {
                LOG(WARNING) << "Fail to delete txn because rowset is already published "
                             << "tablet_id: " << tablet_info.tablet_id << ", txn_id: " << key.second
                             << ", rowset id: " << load_info.rowset->rowset_id()
                             << ", version: " << load_info.rowset->version().first;
                return Status::InternalError(
                        fmt::format("Fail to delete txn because rowset is already published. tablet_id: {}, txn_id: {}",
                                    tablet_info.tablet_id, transaction_id));
            } else {
                (void)RowsetMetaManager::remove(meta, tablet_uid, load_info.rowset->rowset_id());
#ifndef BE_TEST
                StorageEngine::instance()->add_unused_rowset(load_info.rowset);
#endif
                VLOG(3) << "delete transaction from engine successfully."
                        << " partition_id: " << key.first << ", txn_id: " << key.second
                        << ", tablet: " << tablet_info.to_string() << ", rowset: "
                        << (load_info.rowset != nullptr ? load_info.rowset->rowset_id().to_string() : "0");
            }
        }
    }
    it->second.erase(tablet_info);
    if (it->second.empty()) {
        txn_tablet_map.erase(it);
        _clear_txn_partition_map_unlocked(transaction_id, partition_id);
    }
    return Status::OK();
}

void TxnManager::get_tablet_related_txns(TTabletId tablet_id, SchemaHash schema_hash, const TabletUid& tablet_uid,
                                         int64_t* partition_id, std::set<int64_t>* transaction_ids) {
    if (partition_id == nullptr || transaction_ids == nullptr) {
        LOG(WARNING) << "parameter is null when get transactions by tablet";
        return;
    }

    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        std::shared_lock txn_rdlock(_txn_map_locks[i]);
        txn_tablet_map_t& txn_tablet_map = _txn_tablet_maps[i];
        for (auto& it : txn_tablet_map) {
            if (it.second.find(tablet_info) != it.second.end()) {
                *partition_id = it.first.first;
                transaction_ids->insert(it.first.second);
                VLOG(3) << "find transaction on tablet."
                        << "partition_id: " << it.first.first << ", txn_id: " << it.first.second
                        << ", tablet: " << tablet_info.to_string();
            }
        }
    }
}

// force drop all txns related with the tablet
// maybe lock error, because not get txn lock before remove from meta
void TxnManager::force_rollback_tablet_related_txns(KVStore* meta, TTabletId tablet_id, SchemaHash schema_hash,
                                                    const TabletUid& tablet_uid) {
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        std::unique_lock txn_wrlock(_txn_map_locks[i]);
        txn_tablet_map_t& txn_tablet_map = _txn_tablet_maps[i];
        for (auto it = txn_tablet_map.begin(); it != txn_tablet_map.end();) {
            auto load_itr = it->second.find(tablet_info);
            if (load_itr != it->second.end()) {
                TabletTxnInfo& load_info = load_itr->second;
                if (load_info.rowset != nullptr && meta != nullptr) {
                    LOG(INFO) << " delete transaction from engine "
                              << ", tablet: " << tablet_info.to_string()
                              << ", rowset id: " << load_info.rowset->rowset_id();
                    (void)RowsetMetaManager::remove(meta, tablet_uid, load_info.rowset->rowset_id());
                }
                LOG(INFO) << "remove tablet related txn."
                          << " partition_id: " << it->first.first << ", txn_id: " << it->first.second
                          << ", tablet: " << tablet_info.to_string() << ", rowset: "
                          << (load_info.rowset != nullptr ? load_info.rowset->rowset_id().to_string() : "0");
                it->second.erase(tablet_info);
            }
            if (it->second.empty()) {
                _clear_txn_partition_map_unlocked(it->first.second, it->first.first);
                it = txn_tablet_map.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void TxnManager::get_txn_related_tablets(const TTransactionId transaction_id, TPartitionId partition_id,
                                         std::map<TabletInfo, RowsetSharedPtr>* tablet_infos) {
    // get tablets in this transaction
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    std::shared_lock txn_rdlock(_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it == txn_tablet_map.end()) {
        VLOG(3) << "could not find tablet for"
                << " partition_id=" << partition_id << ", txn_id: " << transaction_id;
        return;
    }
    std::map<TabletInfo, TabletTxnInfo>& load_info_map = it->second;

    // each tablet
    for (auto& load_info : load_info_map) {
        const TabletInfo& tablet_info = load_info.first;
        // must not check rowset == null here, because if rowset == null
        // publish version should failed
        tablet_infos->emplace(tablet_info, load_info.second.rowset);
    }
}

void TxnManager::get_all_related_tablets(std::set<TabletInfo>* tablet_infos) {
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        std::shared_lock txn_rdlock(_txn_map_locks[i]);
        for (auto& it : _txn_tablet_maps[i]) {
            for (auto& tablet_load_it : it.second) {
                tablet_infos->emplace(tablet_load_it.first);
            }
        }
    }
}

bool TxnManager::has_txn(TPartitionId partition_id, TTransactionId transaction_id, TTabletId tablet_id,
                         SchemaHash schema_hash, const TabletUid& tablet_uid) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    std::shared_lock txn_rdlock(_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    bool found = it != txn_tablet_map.end() && it->second.find(tablet_info) != it->second.end();

    return found;
}

void TxnManager::build_expire_txn_map(std::map<TabletInfo, std::vector<int64_t>>* expire_txn_map) {
    int64_t now = UnixSeconds();
    // traverse the txn map, and get all expired txns
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        std::shared_lock txn_rdlock(_txn_map_locks[i]);
        for (auto& it : _txn_tablet_maps[i]) {
            auto txn_id = it.first.second;
            for (auto& t_map : it.second) {
                double diff = difftime(now, t_map.second.creation_time);
                if (diff >= config::pending_data_expire_time_sec) {
                    (*expire_txn_map)[t_map.first].push_back(txn_id);
                    if (VLOG_IS_ON(3)) {
                        VLOG(3) << "find expired txn."
                                << " tablet=" << t_map.first.to_string() << " txn_id: " << txn_id
                                << " exist_sec=" << diff;
                    }
                }
            }
        }
    }
}

void TxnManager::get_partition_ids(const TTransactionId transaction_id, std::vector<TPartitionId>* partition_ids) {
    std::shared_lock txn_rdlock(_get_txn_map_lock(transaction_id));
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id);
    auto it = txn_partition_map.find(transaction_id);
    if (it != txn_partition_map.end()) {
        for (int64_t partition_id : it->second) {
            partition_ids->push_back(partition_id);
        }
    }
}

void TxnManager::_insert_txn_partition_map_unlocked(int64_t transaction_id, int64_t partition_id) {
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id);
    auto find = txn_partition_map.find(transaction_id);
    if (find == txn_partition_map.end()) {
        txn_partition_map[transaction_id] = std::unordered_set<int64_t>();
    }
    txn_partition_map[transaction_id].insert(partition_id);
}

void TxnManager::_clear_txn_partition_map_unlocked(int64_t transaction_id, int64_t partition_id) {
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id);
    auto it = txn_partition_map.find(transaction_id);
    if (it != txn_partition_map.end()) {
        it->second.erase(partition_id);
        if (it->second.empty()) {
            txn_partition_map.erase(it);
        }
    }
}

} // namespace starrocks
