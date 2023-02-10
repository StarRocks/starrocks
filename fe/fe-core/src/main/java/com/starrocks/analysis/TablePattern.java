// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.FeNameFormat;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.mysql.privilege.Auth.PrivLevel;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// only the following 3 formats are allowed
// db.tbl
// *.*
// db.*
public class TablePattern implements Writable {
    private String db;
    private String tbl;
    boolean isAnalyzed = false;

    public static TablePattern ALL;

    static {
        ALL = new TablePattern("*", "*");
        try {
            ALL.analyze();
        } catch (AnalysisException e) {
            // will not happen
        }
    }

    private TablePattern() {
    }

    public TablePattern(String db, String tbl) {
        this.db = Strings.isNullOrEmpty(db) ? "*" : db;
        this.tbl = Strings.isNullOrEmpty(tbl) ? "*" : tbl;
    }

    public String getQuolifiedDb() {
        Preconditions.checkState(isAnalyzed);
        return db;
    }

    public String getTbl() {
        return tbl;
    }

    public PrivLevel getPrivLevel() {
        Preconditions.checkState(isAnalyzed);
        if (db.equals("*")) {
            return PrivLevel.GLOBAL;
        } else if (!tbl.equals("*")) {
            return PrivLevel.TABLE;
        } else {
            return PrivLevel.DATABASE;
        }
    }

    public void analyze() throws AnalysisException {
        if (isAnalyzed) {
            return;
        }
        if (db.equals("*") && !tbl.equals("*")) {
            throw new AnalysisException("Do not support format: " + toString());
        }

        if (!db.equals("*")) {
            FeNameFormat.checkDbName(db);
        }

        if (!tbl.equals("*")) {
            FeNameFormat.checkTableName(tbl);
        }
        isAnalyzed = true;
    }

    public static TablePattern read(DataInput in) throws IOException {
        TablePattern tablePattern = new TablePattern();
        tablePattern.readFields(in);
        return tablePattern;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof TablePattern)) {
            return false;
        }
        TablePattern other = (TablePattern) obj;
        return db.equals(other.getQuolifiedDb()) && tbl.equals(other.getTbl());
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + db.hashCode();
        result = 31 * result + tbl.hashCode();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(db).append(".").append(tbl);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Preconditions.checkState(isAnalyzed);
        // compatible with old version
        if (db.equals("*")) {
            Text.writeString(out, db);
        } else {
            Text.writeString(out, ClusterNamespace.getFullName(db));
        }
        Text.writeString(out, tbl);
    }

    public void readFields(DataInput in) throws IOException {
        db = ClusterNamespace.getNameFromFullName(Text.readString(in));
        tbl = Text.readString(in);
        isAnalyzed = true;
    }
}
