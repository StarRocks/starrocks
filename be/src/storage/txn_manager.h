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
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/txn_manager.h

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

#pragma once

#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <pthread.h>
#include <rapidjson/document.h>

#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "agent/status.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "storage/kv_store.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/tablet.h"
#include "util/countdown_latch.h"
#include "util/lru_cache.h"
#include "util/time.h"

namespace starrocks {

struct TxnInfo;

struct TabletTxnInfo {
    PUniqueId load_id;
    RowsetSharedPtr rowset;
    int64_t creation_time{0};
    int64_t commit_time{0};

    TabletTxnInfo(PUniqueId load_id, RowsetSharedPtr rowset)
            : load_id(std::move(load_id)), rowset(std::move(rowset)), creation_time(UnixSeconds()) {}

    TabletTxnInfo() = default;
};

// txn manager is used to manage mapping between tablet and txns
class TxnManager {
public:
    TxnManager(int32_t txn_map_shard_size, int32_t txn_shard_size, uint32_t store_num);

    ~TxnManager() = default;

    [[nodiscard]] Status prepare_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                                     TTransactionId transaction_id, const PUniqueId& load_id);

    [[nodiscard]] Status commit_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                                    TTransactionId transaction_id, const PUniqueId& load_id,
                                    const RowsetSharedPtr& rowset_ptr, bool is_recovery);

    [[nodiscard]] Status publish_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                                     TTransactionId transaction_id, int64_t version, const RowsetSharedPtr& rowset,
                                     uint32_t wait_time = 0);

    // persist_tablet_related_txns persists the tablets' meta and make it crash-safe.
    [[nodiscard]] Status persist_tablet_related_txns(const std::vector<TabletSharedPtr>& tablets);

    // persist metadata of affected_dirs and make it crash-safe
    void flush_dirs(std::unordered_set<DataDir*>& affected_dirs);

    // delete the txn from manager if it is not committed(not have a valid rowset)
    [[nodiscard]] Status rollback_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                                      TTransactionId transaction_id, bool with_log = true);

    [[nodiscard]] Status delete_txn(TPartitionId partition_id, const TabletSharedPtr& tablet,
                                    TTransactionId transaction_id);

    // add a txn to manager
    // partition id is useful in publish version stage because version is associated with partition
    [[nodiscard]] Status prepare_txn(TPartitionId partition_id, TTransactionId transaction_id, TTabletId tablet_id,
                                     SchemaHash schema_hash, const TabletUid& tablet_uid, const PUniqueId& load_id);

    [[nodiscard]] Status commit_txn(KVStore* meta, TPartitionId partition_id, TTransactionId transaction_id,
                                    TTabletId tablet_id, SchemaHash schema_hash, const TabletUid& tablet_uid,
                                    const PUniqueId& load_id, const RowsetSharedPtr& rowset_ptr, bool is_recovery);

    // delete the txn from manager if it is not committed(not have a valid rowset)
    [[nodiscard]] Status rollback_txn(TPartitionId partition_id, TTransactionId transaction_id, TTabletId tablet_id,
                                      SchemaHash schema_hash, const TabletUid& tablet_uid, bool with_log = true);

    // remove the txn from txn manager
    // delete the related rowset if it is not null
    // delete rowset related data if it is not null
    [[nodiscard]] Status delete_txn(KVStore* meta, TPartitionId partition_id, TTransactionId transaction_id,
                                    TTabletId tablet_id, SchemaHash schema_hash, const TabletUid& tablet_uid);

    void get_tablet_related_txns(TTabletId tablet_id, SchemaHash schema_hash, const TabletUid& tablet_uid,
                                 int64_t* partition_id, std::set<int64_t>* transaction_ids);

    void get_txn_related_tablets(const TTransactionId transaction_id, TPartitionId partition_ids,
                                 std::map<TabletInfo, RowsetSharedPtr>* tablet_infos);

    void get_all_related_tablets(std::set<TabletInfo>* tablet_infos);

    // just check if the txn exists
    bool has_txn(TPartitionId partition_id, TTransactionId transaction_id, TTabletId tablet_id, SchemaHash schema_hash,
                 const TabletUid& tablet_uid);

    // get all expired txns and save tham in expire_txn_map.
    // This is currently called before reporting all tablet info, to avoid iterating txn map for every tablets.
    void build_expire_txn_map(std::map<TabletInfo, std::vector<int64_t>>* expire_txn_map);

    void force_rollback_tablet_related_txns(KVStore* meta, TTabletId tablet_id, SchemaHash schema_hash,
                                            const TabletUid& tablet_uid);

    void get_partition_ids(const TTransactionId transaction_id, std::vector<TPartitionId>* partition_ids);

    void get_txn_infos(int64_t txn_id, int64_t tablet_id, std::vector<TxnInfo>& txn_infos);

private:
    using TxnKey = std::pair<int64_t, int64_t>; // partition_id, transaction_id;
    using BThreadCountDownLatch = GenericCountDownLatch<bthread::Mutex, bthread::ConditionVariable>;

    // implement TxnKey hash function to support TxnKey as a key for unordered_map
    struct TxnKeyHash {
        template <typename T, typename U>
        size_t operator()(const std::pair<T, U>& e) const {
            return std::hash<T>()(e.first) ^ std::hash<U>()(e.second);
        }
    };

    // implement TxnKey equal function to support TxnKey as a key for unordered_map
    struct TxnKeyEqual {
        template <class T, typename U>
        bool operator()(const std::pair<T, U>& l, const std::pair<T, U>& r) const {
            return l.first == r.first && l.second == r.second;
        }
    };

    typedef std::unordered_map<TxnKey, std::map<TabletInfo, TabletTxnInfo>, TxnKeyHash, TxnKeyEqual> txn_tablet_map_t;
    typedef std::unordered_map<int64_t, std::unordered_set<int64_t>> txn_partition_map_t;

    std::shared_mutex& _get_txn_map_lock(TTransactionId transactionId);

    txn_tablet_map_t& _get_txn_tablet_map(TTransactionId transactionId);

    txn_partition_map_t& _get_txn_partition_map(TTransactionId transactionId);

    // insert or remove (transaction_id, partition_id) from _txn_partition_map
    // get _txn_map_lock before calling
    void _insert_txn_partition_map_unlocked(int64_t transaction_id, int64_t partition_id);
    void _clear_txn_partition_map_unlocked(int64_t transaction_id, int64_t partition_id);

private:
    const int32_t _txn_map_shard_size;

    const int32_t _txn_shard_size;

    // _txn_map_locks[i] protect _txn_tablet_maps[i], i=0,1,2...,and i < _txn_map_shard_size
    std::unique_ptr<txn_tablet_map_t[]> _txn_tablet_maps;
    // transaction_id -> corresponding partition ids
    // This is mainly for the clear txn task received from FE, which may only has transaction id,
    // so we need this map to find out which partitions are corresponding to a transaction id.
    // The _txn_partition_maps[i] should be constructed/deconstructed/modified alongside with '_txn_tablet_maps[i]'
    std::unique_ptr<txn_partition_map_t[]> _txn_partition_maps;

    std::unique_ptr<std::shared_mutex[]> _txn_map_locks;

    // Dynamic thread pool used to concurrently flush WAL to disk
    std::unique_ptr<ThreadPool> _flush_thread_pool;

    TxnManager(const TxnManager&) = delete;
    const TxnManager& operator=(const TxnManager&) = delete;
}; // TxnManager

inline std::shared_mutex& TxnManager::_get_txn_map_lock(TTransactionId transactionId) {
    return _txn_map_locks[transactionId & (_txn_map_shard_size - 1)];
}

inline TxnManager::txn_tablet_map_t& TxnManager::_get_txn_tablet_map(TTransactionId transactionId) {
    return _txn_tablet_maps[transactionId & (_txn_map_shard_size - 1)];
}

inline TxnManager::txn_partition_map_t& TxnManager::_get_txn_partition_map(TTransactionId transactionId) {
    return _txn_partition_maps[transactionId & (_txn_map_shard_size - 1)];
}

} // namespace starrocks
