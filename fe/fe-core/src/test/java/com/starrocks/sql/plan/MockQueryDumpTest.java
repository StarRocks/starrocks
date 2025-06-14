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

package com.starrocks.sql.plan;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.sql.optimizer.dump.QueryDumpInfo;
import com.starrocks.thrift.TExplainLevel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@RunWith(Parameterized.class)
public class MockQueryDumpTest extends ReplayFromDumpTestBase {
    private String fileName;

    public MockQueryDumpTest(String fileName) {
        this.fileName = fileName;
    }

    @Parameterized.Parameters
    public static Collection<String> data() {
        String folderPath = Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("sql")).getPath()
                + "/query_dump/mock-files";
        File folder = new File(folderPath);
        List<String> fileNames = Lists.newArrayList();

        if (folder.exists() && folder.isDirectory()) {
            File[] files = folder.listFiles();
            for (File file : files) {
                fileNames.add(file.getName().split("\\.")[0]);
            }
        }
        return fileNames;
    }

    @Test
    public void testMockQueryDump() {
        try {
            Pair<QueryDumpInfo, String> replayPair =
                    getPlanFragment(getDumpInfoFromFile("query_dump/mock-files/" + fileName),
                            null, TExplainLevel.NORMAL);
            Assert.assertTrue(replayPair.second.contains("mock"));
        } catch (Throwable e) {
            Assert.fail("file: " + fileName + " should succeed. errMsg: " + e.getMessage());
        }
    }
}
