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

package com.starrocks.connector.fluss;

import com.starrocks.connector.RemoteFileDesc;
import org.apache.fluss.flink.source.split.SourceSplitBase;

import java.util.List;

public class FlussRemoteFileDesc extends RemoteFileDesc {

    private List<SourceSplitBase> flussSplitsInfo;

    private FlussRemoteFileDesc(List<SourceSplitBase> flussSplitsInfo) {
        super(null, null, 0, 0, null);
        this.flussSplitsInfo = flussSplitsInfo;
    }

    public static FlussRemoteFileDesc createFlussRemoteFileDesc(List<SourceSplitBase> paimonSplitsInfo) {
        return new FlussRemoteFileDesc(paimonSplitsInfo);
    }

    public List<SourceSplitBase> getFlussSplitsInfo() {
        return flussSplitsInfo;
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
                ", flussSplitsInfo=" + flussSplitsInfo +
                '}';
    }
}
