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

import com.starrocks.connector.RemoteMetaSplit;
import com.starrocks.connector.share.iceberg.ManifestFileBean;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.util.SerializationUtil;

public class IcebergMetaSplit implements RemoteMetaSplit {
    private final String manifestFile;
    private final long length;
    private final String path;

    public static IcebergMetaSplit from(ManifestFile file) {
        ManifestFileBean bean = ManifestFileBean.fromManifest(file);
        String serializedFile = SerializationUtil.serializeToBase64(bean);
        return new IcebergMetaSplit(serializedFile, file.length(), file.path());
    }

    public IcebergMetaSplit(String manifestFile, long length, String path) {
        this.manifestFile = manifestFile;
        this.length = length;
        this.path = path;
    }

    @Override
    public String getSerializeSplit() {
        return manifestFile;
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public String path() {
        return path;
    }
}
