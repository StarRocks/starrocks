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
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.connector.share.iceberg.ManifestFileBean;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.util.SerializationUtil;

import java.util.List;

public class IcebergMetaSplit implements RemoteMetaSplit {
    public static final String PLACEHOLDER_FILE = "placeholder_file";
    public static final long PLACEHOLDER_FILE_LENGTH = 1L;
    public static final String PLACEHOLDER_FILE_PATH = "placeholder_file_path";
    public static final List<MetadataTableType> ONLY_NEED_SINGLE_SPLIT = List.of(
            MetadataTableType.REFS,
            MetadataTableType.HISTORY,
            MetadataTableType.METADATA_LOG_ENTRIES,
            MetadataTableType.SNAPSHOTS,
            MetadataTableType.MANIFESTS);

    private final String manifestFile;
    private final long length;
    private final String path;

    public static IcebergMetaSplit from(ManifestFile file) {
        ManifestFileBean bean = ManifestFileBean.fromManifest(file);
        String serializedFile = SerializationUtil.serializeToBase64(bean);
        return new IcebergMetaSplit(serializedFile, file.length(), file.path());
    }

    // A placeholder split for some lightweight metadata table query
    public static IcebergMetaSplit placeholderSplit() {
        return new IcebergMetaSplit(PLACEHOLDER_FILE, PLACEHOLDER_FILE_LENGTH, PLACEHOLDER_FILE_PATH);
    }

    public static boolean onlyNeedSingleSplit(MetadataTableType tableType) {
        return ONLY_NEED_SINGLE_SPLIT.contains(tableType);
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
