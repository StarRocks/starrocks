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

#pragma once

#include <span>

#include "common/statusor.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks {
class TxnInfoPB;
}

namespace starrocks::lake {

class TabletManager;

// Publish a new version of tablet metadata by applying a set of transactions.
//
// This function does the following:
//
// 1. Load the base tablet metadata with id 'tablet_id' and version 'base_version'.
// 2. Read the transaction logs for all 'txns' sequentially and apply them to the base metadata.
// 3. Save the result as a new tablet metadata with version 'new_version'.
// 4. Update the metadata's commit timestamp to 'commit_time'.
// 5. Persist the new metadata to the object storage.
//
// Parameters:
// - tablet_mgr A pointer to the TabletManager object managing the tablet, cannot be nullptr
// - tablet_id Id of the tablet
// - base_version Version of the base metadata
// - new_version The new version to be published
// - txns Transactions to apply in sequence
// - commit_time New commit timestamp
//
// Return:
// - StatusOr containing the new published TabletMetadataPtr on success.
StatusOr<TabletMetadataPtr> publish_version(TabletManager* tablet_mgr, int64_t tablet_id, int64_t base_version,
                                            int64_t new_version, std::span<const TxnInfoPB> txns);

// Publish a batch new versions of transaction logs.
//
// For every transaction log, this function does the following:
// 1. copy the transaction log identified by 'txn_id' to a new file identified by 'log_version'
// 2. Delete the transaction log identified by 'txn_id' in an asynchronous manner
//
// Parameters:
// - tablet_mgr A pointer to the TabletManager object managing the tablet, cannot be nullptr
// - tablet_id Id of the tablet
// - txn_infos Transactions to apply
// - log_version Version of the new file
//
// Return:
// - Returns OK if the copy was successful, asynchronous deletion does not affect the return value.
Status publish_log_version(TabletManager* tablet_mgr, int64_t tablet_id, std::span<const TxnInfoPB> txn_infos,
                           const int64_t* log_versions);

// Aborts a transaction with the specified transaction IDs on the given tablet.
//
// This function does the following:
//
// 1. Read the transaction logs for all 'txn_ids' sequentially
// 2. Collects the list of files logged in the transaction logs
// 3. Delete all collected files and transaction logs
//
// Parameters:
// - tablet_mgr A pointer to the TabletManager object managing the tablet, cannot be nullptr
// - tablet_id The ID of the tablet where the transaction will be aborted.
// - txns A `std::span` of `TxnInfoPB` containing information of the transactions to be aborted.
//
void abort_txn(TabletManager* tablet_mgr, int64_t tablet_id, std::span<const TxnInfoPB> txns);

} // namespace starrocks::lake
