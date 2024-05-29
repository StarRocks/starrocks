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

package com.starrocks.sql.automv.tunespace;

import com.starrocks.sql.automv.qe.TablePlus;

import java.util.Objects;

public class TuneSpace {

    private final String db;
    private final String tableName;
    private final TablePlus table;

    private TuneSpace(String db, String tableName, int numBuckets, int replicationNum) {
        this.db = Objects.requireNonNull(db);
        this.tableName = Objects.requireNonNull(tableName);
        this.table = PlanPieceInfo.getTable(db + "." + tableName, numBuckets, replicationNum);
    }

    public static TuneSpace of(String db, String tableName, int numBuckets, int replicationNum) {
        return new TuneSpace(db, tableName, numBuckets, replicationNum);
    }

    public String getDb() {
        return db;
    }

    public String getTableName() {
        return tableName;
    }

    public TablePlus getTable() {
        return table;
    }
}
