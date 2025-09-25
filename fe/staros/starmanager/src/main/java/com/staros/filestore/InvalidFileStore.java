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

import com.staros.proto.FileStoreInfo;
import com.staros.proto.FileStoreType;

public class InvalidFileStore extends AbstractFileStore {
    public static final String INVALID_KEY_PREFIX = "invalid_fstype";

    public InvalidFileStore(FileStoreInfo fsInfo) {
        super(fsInfo);
    }

    @Override
    public FileStoreType type() {
        return FileStoreType.INVALID;
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public String rootPath() {
        return "";
    }

    public static FileStore fromProtobuf(FileStoreInfo fsInfo) {
        InvalidFileStore fs = new InvalidFileStore(fsInfo);
        fs.key = fs.INVALID_KEY_PREFIX + "_" + fsInfo.getFsType();
        return fs;
    }
}
