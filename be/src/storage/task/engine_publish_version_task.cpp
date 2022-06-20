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

Status EnginePublishVersionTask::finish() {
    VLOG(1) << "begin to publish version on tablet. "
            << "tablet_id=" << _tablet_info.tablet_id << ", schema_hash=" << _tablet_info.schema_hash
            << ", version=" << _version << ", txn_id: " << _transaction_id;

    TabletSharedPtr tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(_tablet_info.tablet_id, _tablet_info.tablet_uid);

    if (tablet == nullptr) {
        LOG(WARNING) << "Not found tablet to publish_version. tablet_id: " << _tablet_info.tablet_id
                     << ", txn_id: " << _transaction_id;
        return Status::NotFound(fmt::format("Not found tablet to publish_version. tablet_id: {}, txn_id: {}",
                                            _tablet_info.tablet_id, _transaction_id));
    }

    Status st = Status::OK();
    if (tablet->keys_type() == KeysType::PRIMARY_KEYS) {
        VLOG(1) << "UpdateManager::on_rowset_published tablet:" << tablet->tablet_id()
                << " rowset: " << _rowset->rowset_id().to_string() << " version: " << _version;
        st = StorageEngine::instance()->txn_manager()->publish_txn2(_transaction_id, _partition_id, tablet, _version);
    } else {
        st = StorageEngine::instance()->txn_manager()->publish_txn(_partition_id, tablet, _transaction_id,
                                                                   Version{_version, _version});
    }
    if (!st.ok()) {
        LOG(WARNING) << "Failed to publish version. rowset_id=" << _rowset->rowset_id()
                     << ", tablet_id=" << _tablet_info.tablet_id << ", txn_id: " << _transaction_id;
        return st;
    }

    if (tablet->keys_type() != KeysType::PRIMARY_KEYS) {
        // add visible rowset to tablet
        auto st = tablet->add_inc_rowset(_rowset);
        if (!st.ok() && !st.is_already_exist()) {
            LOG(WARNING) << "fail to add visible rowset to tablet. rowset_id=" << _rowset->rowset_id()
                         << ", tablet_id=" << _tablet_info.tablet_id << ", txn_id: " << _transaction_id
                         << ", res=" << st;
            return st;
        }
    }
    VLOG(1) << "Publish version successfully on tablet. tablet=" << tablet->full_name()
            << ", txn_id: " << _transaction_id << ", version=" << _version << ", res=" << st.to_string();
    return Status::OK();
}

} // namespace starrocks
