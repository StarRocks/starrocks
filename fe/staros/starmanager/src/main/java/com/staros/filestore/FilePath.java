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


package com.staros.filestore;

import com.staros.proto.FilePathInfo;
import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FilePath {
    private static final Logger LOG = LogManager.getLogger(FilePath.class);

    public FileStore fs;
    public String suffix;

    public FilePath(FileStore fs, String suffix) {
        this.fs = fs;
        this.suffix = suffix;
    }

    public String fullPath() {
        if (fs.rootPath().endsWith("/")) {
            return String.format("%s%s", fs.rootPath(), suffix);
        } else {
            return String.format("%s/%s", fs.rootPath(), suffix);
        }
    }

    public static FilePath fromFullPath(FileStore fs, String fullPath) {
        String suffix = fullPath.substring(fs.rootPath().length());
        if (suffix.startsWith("/")) {
            suffix = suffix.substring(1);
        }
        return new FilePath(fs, suffix);
    }

    public static FilePath fromProtobuf(FilePathInfo path) {
        FileStoreInfo fsInfo = path.getFsInfo();
        FileStoreType fsType = fsInfo.getFsType();
        FileStore fs = FileStore.fromProtobuf(fsInfo);
        return FilePath.fromFullPath(fs, path.getFullPath());
    }

    private FilePathInfo toProtobufInternal(boolean includeSecret) {
        FilePathInfo.Builder builder = FilePathInfo.newBuilder();
        if (includeSecret) {
            builder.setFsInfo(fs.toProtobuf());
        } else {
            builder.setFsInfo(fs.toDebugProtobuf());
        }
        builder.setFullPath(fullPath());
        return builder.build();
    }

    public FilePathInfo toProtobuf() {
        return toProtobufInternal(true /* includeSecret */);
    }

    public FilePathInfo toDebugProtobuf() {
        return toProtobufInternal(false /* includeSecret */);
    }
}
