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

package com.starrocks.lake;

import com.starrocks.lake.compaction.CompactionTxnCommitAttachment;
import com.starrocks.proto.TxnInfoPB;
import com.starrocks.transaction.TransactionState;

public class TxnInfoHelper {
    public static TxnInfoPB fromTransactionState(TransactionState state) {
        TxnInfoPB infoPB = new TxnInfoPB();
        infoPB.txnId = state.getTransactionId();
        infoPB.combinedTxnLog = state.isUseCombinedTxnLog();
        infoPB.commitTime = state.getCommitTime() / 1000; // milliseconds to seconds
        infoPB.txnType = state.getTransactionType().toProto();
        // check whether needs to force publish
        if (state.getSourceType() == TransactionState.LoadJobSourceType.LAKE_COMPACTION &&
                state.getTxnCommitAttachment() != null) {
            CompactionTxnCommitAttachment attachment = (CompactionTxnCommitAttachment) state.getTxnCommitAttachment();
            infoPB.forcePublish = attachment.getForceCommit();
        } else {
            infoPB.forcePublish = false;
        }
        infoPB.setGtid(state.getGlobalTransactionId());
        return infoPB;
    }
}
