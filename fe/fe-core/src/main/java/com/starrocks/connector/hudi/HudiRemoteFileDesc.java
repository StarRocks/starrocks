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

package com.starrocks.connector.hudi;

import com.google.common.collect.ImmutableList;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import org.apache.hudi.common.table.timeline.HoodieInstant;

public class HudiRemoteFileDesc extends RemoteFileDesc {
    private final ImmutableList<String> hudiDeltaLogs;
    private final HoodieInstant hudiInstant;

    private HudiRemoteFileDesc(String fileName, long length, ImmutableList<RemoteFileBlockDesc> blockDescs,
                                 ImmutableList<String> hudiDeltaLogs, HoodieInstant lastInstant) {
        super(fileName, "", length, 0, blockDescs);
        this.hudiDeltaLogs = hudiDeltaLogs;
        this.hudiInstant = lastInstant;
    }

    public static HudiRemoteFileDesc createHudiRemoteFileDesc(String fileName, long length,
                                                              ImmutableList<RemoteFileBlockDesc> blockDescs,
                                                            ImmutableList<String> hudiDeltaLogs,
                                                            HoodieInstant lastInstant) {
        return new HudiRemoteFileDesc(fileName, length, blockDescs, hudiDeltaLogs, lastInstant);
    }

    public ImmutableList<String> getHudiDeltaLogs() {
        return hudiDeltaLogs;
    }


    public HoodieInstant getHudiInstant() {
        return hudiInstant;
    }

    @Override
    public String toString() {
        return "RemoteFileDesc{" + "fileName='" + fileName + '\'' +
                "fullPath='" + fullPath + '\'' +
                ", compression='" + compression + '\'' +
                ", length=" + length +
                ", modificationTime=" + modificationTime +
                ", blockDescs=" + blockDescs +
                ", splittable=" + splittable +
                ", hudiDeltaLogs=" + hudiDeltaLogs +
                '}';
    }
}
