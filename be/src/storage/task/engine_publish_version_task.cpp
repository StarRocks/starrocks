// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/task/engine_publish_version_task.cpp

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

#include "storage/task/engine_publish_version_task.h"

#include "storage/data_dir.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/tablet_manager.h"
#include "storage/update_manager.h"

namespace starrocks {

EnginePublishVersionTask::EnginePublishVersionTask(TTransactionId transaction_id, TPartitionId partition_id,
                                                   TVersion version, const TabletInfo& tablet_info,
                                                   const RowsetSharedPtr& rowset)
        : _transaction_id(transaction_id),
          _partition_id(partition_id),
          _version(version),
          _tablet_info(tablet_info),
          _rowset(rowset) {}

Status EnginePublishVersionTask::execute() {
    VLOG(1) << "Begin publish txn tablet:" << _tablet_info.tablet_id << " version:" << _version
            << " partition:" << _partition_id << " txn_id: " << _transaction_id << " rowset:" << _rowset->rowset_id();
    int64_t start_ts = MonotonicMillis();

    TabletSharedPtr tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_info.tablet_id, _tablet_info.tablet_uid);

    if (tablet == nullptr) {
        LOG(WARNING) << "Not found tablet to publish_version. tablet_id: " << _tablet_info.tablet_id
                     << ", txn_id: " << _transaction_id;
        return Status::NotFound(fmt::format("Not found tablet to publish_version. tablet_id: {}, txn_id: {}",
                                            _tablet_info.tablet_id, _transaction_id));
    }

    int64_t t1 = MonotonicMillis() - start_ts;

    auto st = StorageEngine::instance()->txn_manager()->publish_txn(_partition_id, tablet, _transaction_id, _version,
                                                                    _rowset);
    int64_t t2 = MonotonicMillis() - start_ts;

    if (!st.ok()) {
        LOG(WARNING) << "Publish txn failed tablet:" << _tablet_info.tablet_id << " version:" << _version
                     << " partition:" << _partition_id << " txn_id: " << _transaction_id
                     << " rowset:" << _rowset->rowset_id();
    } else {
        LOG(INFO) << "Publish txn success tablet:" << _tablet_info.tablet_id << " version:" << _version
                  << " partition:" << _partition_id << " txn_id: " << _transaction_id
                  << " rowset:" << _rowset->rowset_id() << "time:" << t1 << "," << t2;
    }
    return st;
}

} // namespace starrocks
