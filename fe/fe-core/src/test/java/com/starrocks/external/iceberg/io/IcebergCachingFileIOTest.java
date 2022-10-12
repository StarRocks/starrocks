// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.external.iceberg.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hadoop.HadoopFileIO;
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
    public void testnewInputFile() {
        writeIcebergMetaTestFile();
        String path = "file:/tmp/0001.metadata.json";

        // create hadoopFileIO
        HadoopFileIO hadoopFileIO = new HadoopFileIO(new Configuration());
        // create iceberg cachingFileIO
        IcebergCachingFileIO cachingFileIO = new IcebergCachingFileIO(hadoopFileIO);
        Map<String, String> icebergProperties = new HashMap<>();
        cachingFileIO.initialize(icebergProperties);

        // get inputfile by hadoopFileIO and cachingFileIO
        InputFile hadoopFileIOInputFile = hadoopFileIO.newInputFile(path);
        InputFile cachingFileIOInputFile = cachingFileIO.newInputFile(path);
        cachingFileIOInputFile.newStream();

        String cachingFileIOPath = cachingFileIOInputFile.location();
        String hadoopFileIOPath = hadoopFileIOInputFile.location();
        Assert.assertEquals(path, hadoopFileIOPath);
        Assert.assertEquals(path, cachingFileIOPath);

        long cacheIOInputFileSize = cachingFileIOInputFile.getLength();
        long hadoopIOInputFileSize = hadoopFileIOInputFile.getLength();
        Assert.assertEquals(cacheIOInputFileSize, 39);
        Assert.assertEquals(hadoopIOInputFileSize, 39);
    }
}