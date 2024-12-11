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

package com.starrocks.connector.kudu;

import com.starrocks.connector.RemoteFileDesc;
import org.apache.kudu.client.KuduScanToken;

import java.util.List;

public class KuduRemoteFileDesc extends RemoteFileDesc {
    private final List<KuduScanToken> kuduScanTokens;

    private KuduRemoteFileDesc(List<KuduScanToken> kuduScanTokens) {
        super(null, null, 0, 0, null);
        this.kuduScanTokens = kuduScanTokens;
    }

    public static KuduRemoteFileDesc createKuduRemoteFileDesc(List<KuduScanToken> kuduScanTokens) {
        return new KuduRemoteFileDesc(kuduScanTokens);
    }

    public List<KuduScanToken> getKuduScanTokens() {
        return kuduScanTokens;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RemoteFileDesc{");
        sb.append("fileName='").append(fileName).append('\'');
        sb.append("fullPath='").append(fullPath).append('\'');
        sb.append(", compression='").append(compression).append('\'');
        sb.append(", length=").append(length);
        sb.append(", modificationTime=").append(modificationTime);
        sb.append(", blockDescs=").append(blockDescs);
        sb.append(", splittable=").append(splittable);
        sb.append(", textFileFormatDesc=").append(textFileFormatDesc);
        sb.append(", kuduScanTokens=").append(kuduScanTokens);
        sb.append('}');
        return sb.toString();
    }
}
