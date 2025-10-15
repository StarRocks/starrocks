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

package com.starrocks.fs.hdfs;

import com.starrocks.common.StarRocksException;
import com.starrocks.fs.FileSystem;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.thrift.THdfsProperties;
import org.apache.hadoop.fs.FileStatus;

import java.util.List;
import java.util.Map;

public class HdfsFileSystemWrap implements FileSystem {
    private final Map<String, String> properties;

    public HdfsFileSystemWrap(Map<String, String> properties) {
        this.properties = properties;
    }

    @Override
    public List<FileStatus> globList(String path, boolean skipDir) throws StarRocksException {
        return HdfsUtil.listFileMeta(path, properties, skipDir);
    }

    @Override
    public THdfsProperties getHdfsProperties(String path) throws StarRocksException {
        THdfsProperties hdfsProperties = new THdfsProperties();
        HdfsUtil.getTProperties(path, properties, hdfsProperties);
        return hdfsProperties;
    }
}
