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

package com.starrocks.connector.iceberg.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.io.InputFile;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IcebergCachingFileIOTest {

    public void writeIcebergMetaTestFile() {
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter("/tmp/0001.metadata.json"));
            out.write("test iceberg metadata json file content");
            out.close();
        } catch (IOException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testNewInputFile() {
        writeIcebergMetaTestFile();
        String path = "file:/tmp/0001.metadata.json";

        // create iceberg cachingFileIO
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO();
        cachingFileIO.setConf(new Configuration());
        Map<String, String> icebergProperties = new HashMap<>();
        icebergProperties.put("iceberg.catalog.type", "hive");
        cachingFileIO.initialize(icebergProperties);

        InputFile cachingFileIOInputFile = cachingFileIO.newInputFile(path);
        cachingFileIOInputFile.newStream();

        String cachingFileIOPath = cachingFileIOInputFile.location();
        Assert.assertEquals(path, cachingFileIOPath);

        long cacheIOInputFileSize = cachingFileIOInputFile.getLength();
        Assert.assertEquals(cacheIOInputFileSize, 39);
        cachingFileIO.deleteFile(path);
    }
}