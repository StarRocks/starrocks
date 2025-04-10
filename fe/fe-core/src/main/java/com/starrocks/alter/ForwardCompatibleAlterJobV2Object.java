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

package com.starrocks.alter;

import com.starrocks.persist.gson.IForwardCompatibleObject;

import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class ForwardCompatibleAlterJobV2Object extends AlterJobV2 implements IForwardCompatibleObject {
    public ForwardCompatibleAlterJobV2Object(long jobId, JobType jobType, long dbId, long tableId, String tableName,
                                             long timeoutMs) {
        super(jobId, jobType, dbId, tableId, tableName, timeoutMs);
    }

    protected ForwardCompatibleAlterJobV2Object(JobType type) {
        super(type);
    }

    @Override
    protected void runPendingJob() throws AlterCancelException {
        throw new AlterCancelException("Not supported job type");
    }

    @Override
    protected void runWaitingTxnJob() throws AlterCancelException {
        throw new AlterCancelException("Not supported job type");
    }

    @Override
    protected void runRunningJob() throws AlterCancelException {
        throw new AlterCancelException("Not supported job type");
    }

    @Override
    protected void runFinishedRewritingJob() throws AlterCancelException {
        throw new AlterCancelException("Not supported job type");
    }

    @Override
    protected boolean cancelImpl(String errMsg) {
        return false;
    }

    @Override
    protected void getInfo(List<List<Comparable>> infos) {
    }

    @Override
    public void replay(AlterJobV2 replayedJob) {
    }

    @Override
    public Optional<Long> getTransactionId() {
        return Optional.empty();
    }

    @Override
    public void write(DataOutput out) throws IOException {
    }
}
