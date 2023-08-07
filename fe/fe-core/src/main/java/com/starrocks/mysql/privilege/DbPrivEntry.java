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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/DbPrivEntry.java

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

package com.starrocks.mysql.privilege;

import com.starrocks.analysis.TablePattern;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.io.Text;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.GrantPrivilegeStmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DbPrivEntry extends PrivEntry {
    protected static final String ANY_DB = "*";

    protected PatternMatcher dbPattern;
    protected String origDb;
    protected boolean isAnyDb;

    protected DbPrivEntry() {
    }

    protected DbPrivEntry(String origHost, String user, boolean isDomain, PrivBitSet privSet, String origDb) {
        super(origHost, user, isDomain, privSet);
        this.origDb = origDb;
    }

    @Override
    protected void analyse() throws AnalysisException {
        super.analyse();

        this.dbPattern = createDbPatternMatcher(this.origDb);
        if (origDb.equals(ANY_DB)) {
            isAnyDb = true;
        }
    }

    public static DbPrivEntry create(String host, String db, String user, boolean isDomain, PrivBitSet privs)
            throws AnalysisException {
        DbPrivEntry dbPrivEntry = new DbPrivEntry(host, user, isDomain, privs, db);
        dbPrivEntry.analyse();
        return dbPrivEntry;
    }

    private static PatternMatcher createDbPatternMatcher(String db) throws AnalysisException {
        // the database 'information_schema''s name is case insensibility.
        boolean dbCaseSensibility = CaseSensibility.DATABASE.getCaseSensibility();
        if (ClusterNamespace.getNameFromFullName(db).equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {
            dbCaseSensibility = false;
        }

        PatternMatcher dbPattern = PatternMatcher.createMysqlPattern(db.equals(ANY_DB) ? "%" : db, dbCaseSensibility);
        return dbPattern;
    }

    public PatternMatcher getDbPattern() {
        return dbPattern;
    }

    public String getOrigDb() {
        return origDb;
    }

    public boolean isAnyDb() {
        return isAnyDb;
    }

    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof DbPrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        DbPrivEntry otherEntry = (DbPrivEntry) other;
        int res = otherEntry.origHost.compareTo(origHost);
        if (res != 0) {
            return res;
        }

        res = otherEntry.origDb.compareTo(origDb);
        if (res != 0) {
            return res;
        }

        return otherEntry.realOrigUser.compareTo(realOrigUser);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof DbPrivEntry)) {
            return false;
        }

        DbPrivEntry otherEntry = (DbPrivEntry) other;
        if (origHost.equals(otherEntry.origHost) && realOrigUser.equals(otherEntry.realOrigUser)
                && origDb.equals(otherEntry.origDb) && isDomain == otherEntry.isDomain) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("db priv. host: ").append(origHost).append(", db: ").append(origDb);
        sb.append(", user: ").append(realOrigUser);
        sb.append(", priv: ").append(privSet).append(", set by resolver: ").append(isSetByDomainResolver);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = DbPrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        super.write(out);
        // compatible with old version
        Text.writeString(out, ClusterNamespace.getFullName(origDb));
        isClassNameWrote = false;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        origDb = ClusterNamespace.getNameFromFullName(Text.readString(in));
    }

    @Override
    public String toGrantSQL() {
        GrantPrivilegeStmt stmt = new GrantPrivilegeStmt(null, "TABLE", getUserIdent(), false);
        stmt.setAnalysedTable(privSet, new TablePattern(origDb, "*"));
        return AstToStringBuilder.toString(stmt);
    }

}
