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

package com.starrocks.schema;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.parquet.Strings;

import java.util.List;
import java.util.stream.Collectors;

/**
 * MTable means Mocked-Table and is used to mock SR's table so can be used for FE's unit tests.
 */
public class MTable {
    private String tableName;
    private List<String> columns;
    private String distCol;
    private String partCol;
    private String duplicateKeys;
    private List<String> partKeys;
    private int bucketNum = 3;
    private int replicateNum = 1;
    private List<String> values = Lists.newArrayList();
    private List<String> properties = Lists.newArrayList();

    public MTable(String tableName, List<String> columns) {
        this(tableName, "", columns);
    }

    public MTable(String tableName, String distCol, List<String> columns) {
        this(tableName, distCol, columns, "", Lists.newArrayList());
    }

    public MTable(String tableName, String distCol, List<String> columns, String partCol, List<String> partKeys) {
        this(tableName, distCol, columns, partCol, partKeys, "");
    }
    public MTable(String tableName, String distCol, List<String> columns,
                  String partCol, List<String> partKeys,
                  String duplicateKeys) {
        this.tableName = tableName;
        this.duplicateKeys = duplicateKeys;
        this.columns = columns;
        this.distCol = distCol;
        this.partCol = partCol;
        this.partKeys = partKeys;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public String getDistCol() {
        return distCol;
    }

    public void setDistCol(String distCol) {
        this.distCol = distCol;
    }

    public int getBucketNum() {
        return bucketNum;
    }

    public void setBucketNum(int bucketNum) {
        this.bucketNum = bucketNum;
    }

    public int getReplicateNum() {
        return replicateNum;
    }

    public void setReplicateNum(int replicateNum) {
        this.replicateNum = replicateNum;
    }

    public MTable withValues(String value) {
        String[] arr = value.split(",");
        return withValues(Lists.newArrayList(arr));
    }

    public MTable withValues(List<String> values) {
        this.values = values;
        return this;
    }

    public MTable withProperties(String value) {
        String[] arr = value.split(",");
        return withProperties(Lists.newArrayList(arr));
    }

    public MTable withProperties(List<String> values) {
        this.properties = values;
        return this;
    }

    public String getCreateTableSql() {
        String  sql = String.format("CREATE TABLE `%s` (\n" +
                        " %s" +
                        "\n" +
                        ") ENGINE=OLAP\n",
                this.tableName, Joiner.on(",\n  ").join(columns));
        if (!Strings.isNullOrEmpty(duplicateKeys)) {
            sql += String.format("DUPLICATE KEY (%s) \n", duplicateKeys);
        }
        if (!Strings.isNullOrEmpty(partCol)) {
            sql += String.format("PARTITION BY RANGE(`%s`)\n", partCol);
            sql += String.format("(\n%s\n)\n", Joiner.on(",\n   ").join(partKeys));
        }
        if (Strings.isNullOrEmpty(distCol)) {
            sql += String.format("DISTRIBUTED BY RANDOM BUCKETS %s\n", bucketNum);
        } else {
            sql += String.format("DISTRIBUTED BY HASH(`%s`) BUCKETS %s\n", distCol, bucketNum);
        }
        if (properties == null || properties.isEmpty()) {
            sql += String.format("PROPERTIES (\n" +
                    "\"replication_num\" = \"%s\"\n" +
                    ");", replicateNum);
        } else {
            List<String> newProperties = properties.stream().map(p -> {
                StringBuilder sb = new StringBuilder();
                String format = p.replace("'", "\"");
                sb.append(format);
                return sb.toString();
            }).collect(Collectors.toList());
            sql += String.format("PROPERTIES (\n" +
                    "\"replication_num\" = \"%s\",\n" +
                    "%s\n" +
                    ");",
                    replicateNum,
                    Joiner.on(",\n").join(newProperties));
        }
        return sql;
    }

    public String getGenerateDataSQL() {
        if (values == null || values.isEmpty()) {
            return null;
        }
        return String.format("INSERT INTO %s VALUES %s", this.tableName, Joiner.on(",").join(values));
    }
}
