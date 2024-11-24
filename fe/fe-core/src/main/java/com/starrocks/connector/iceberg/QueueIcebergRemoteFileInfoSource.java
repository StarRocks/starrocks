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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.RemoteFileInfo;
import com.starrocks.connector.RemoteFileInfoSource;

import java.util.Deque;

public class QueueIcebergRemoteFileInfoSource implements RemoteFileInfoSource {
    private final IcebergRemoteSourceTrigger sourceTrigger;
    private final Deque<RemoteFileInfo> queue;

    public QueueIcebergRemoteFileInfoSource(IcebergRemoteSourceTrigger sourceTrigger, Deque<RemoteFileInfo> queue) {
        this.sourceTrigger = sourceTrigger;
        this.queue = queue;
    }

    @Override
    public RemoteFileInfo getOutput() {
        return queue.pop();
    }

    @Override
    public boolean hasMoreOutput() {
        if (!queue.isEmpty()) {
            return true;
        }

        while (sourceTrigger.hasMoreOutput()) {
            sourceTrigger.trigger();
            if (!queue.isEmpty()) {
                return true;
            }
        }

        return false;
    }
}
