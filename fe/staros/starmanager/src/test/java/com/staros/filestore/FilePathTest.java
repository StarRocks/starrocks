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
import org.junit.Assert;
import org.junit.Test;

public class FilePathTest {
    @Test
    public void testInvalidFileStore() {
        FileStoreInfo info = FileStoreInfo.newBuilder()
                .setFsKey("test-fskey")
                .setFsType(FileStoreType.INVALID).build();
        FilePathInfo path = FilePathInfo.newBuilder()
                .setFsInfo(info)
                .setFullPath("test").build();
        FilePath filePath = FilePath.fromProtobuf(path);
        Assert.assertEquals("test", filePath.suffix);
        Assert.assertEquals(FileStoreType.INVALID, filePath.fs.type());
        Assert.assertEquals("", filePath.fs.rootPath());
        Assert.assertEquals(InvalidFileStore.INVALID_KEY_PREFIX + "_" + FileStoreType.INVALID, filePath.fs.key());
        Assert.assertEquals(true, filePath.fs.isValid());
    }
}

