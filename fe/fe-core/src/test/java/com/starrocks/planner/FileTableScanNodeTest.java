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

package com.starrocks.planner;

import com.google.common.collect.ImmutableList;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.analysis.TupleId;
import com.starrocks.catalog.FileTable;
import com.starrocks.common.exception.DdlException;
import com.starrocks.connector.RemoteFileBlockDesc;
import com.starrocks.connector.RemoteFileDesc;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileTableScanNodeTest {
    @Test
    public void testSetupScanRangeLocations() throws Exception {
        class ExtFileTable extends FileTable {
            public ExtFileTable(Map<String, String> properties)
                    throws DdlException {
                super(0, "XX", new ArrayList<>(), properties);
            }

            @Override
            public List<RemoteFileDesc> getFileDescsFromHdfs() throws DdlException {
                List<RemoteFileDesc> fileDescList = new ArrayList<>();
                List<RemoteFileBlockDesc> blockDescList = new ArrayList<>();

                class Block extends RemoteFileBlockDesc {
                    public Block() {
                        super(0, 0, new long[] {10}, new long[] {20}, null);
                    }

                    @Override
                    public String getDataNodeIp(long hostId) {
                        return "XXX";
                    }
                }
                blockDescList.add(new Block());
                RemoteFileDesc fileDesc =
                        new RemoteFileDesc("aa", "snappy", 0, 0, ImmutableList.copyOf(blockDescList), null);
                fileDescList.add(fileDesc);
                return fileDescList;
            }
        }

        Map<String, String> properties = new HashMap<String, String>() {
            {
                put(FileTable.JSON_KEY_FILE_PATH, "hdfs://127.0.0.1:10000/hive/");
                put(FileTable.JSON_KEY_COLUMN_SEPARATOR, "XXX");
                put(FileTable.JSON_KEY_ROW_DELIMITER, "YYY");
                put(FileTable.JSON_KEY_COLLECTION_DELIMITER, "ZZZ");
                put(FileTable.JSON_KEY_MAP_DELIMITER, "MMM");
                put(FileTable.JSON_KEY_FORMAT, "text");
            }
        };
        FileTable table = new ExtFileTable(properties);

        TupleDescriptor desc = new TupleDescriptor(new TupleId(0));
        desc.setTable(table);
        FileTableScanNode scanNode = new FileTableScanNode(new PlanNodeId(0), desc, "XXX");
        scanNode.setupScanRangeLocations();
    }
}