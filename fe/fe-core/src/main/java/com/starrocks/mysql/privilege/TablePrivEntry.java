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
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.io.Text;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.GrantPrivilegeStmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TablePrivEntry extends DbPrivEntry {
    public static final String ANY_TBL = "*";

    private PatternMatcher tblPattern;
    private String origTbl;
    private boolean isAnyTbl;

    protected TablePrivEntry() {
    }

    private TablePrivEntry(String origHost, String user, boolean isDomain, PrivBitSet privSet, String db, String origTbl) {
        super(origHost, user, isDomain, privSet, db);
        this.origTbl = origTbl;
    }

    @Override
    protected void analyse() throws AnalysisException {
        super.analyse();

        tblPattern = PatternMatcher.createMysqlPattern(origTbl.equals(ANY_TBL) ? "%" : origTbl,
                CaseSensibility.TABLE.getCaseSensibility());
        if (origTbl.equals(ANY_TBL)) {
            isAnyTbl = true;
        }
    }

    public static TablePrivEntry create(String host, String db, String user, String tbl, boolean isDomain,
                                        PrivBitSet privs) throws AnalysisException {

        TablePrivEntry tablePrivEntry = new TablePrivEntry(host, user, isDomain, privs, db, tbl);
        tablePrivEntry.analyse();
        return tablePrivEntry;
    }

    public PatternMatcher getTblPattern() {
        return tblPattern;
    }

    public String getOrigTbl() {
        return origTbl;
    }

    public boolean isAnyTbl() {
        return isAnyTbl;
    }

    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof TablePrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        TablePrivEntry otherEntry = (TablePrivEntry) other;
        int res = otherEntry.origHost.compareTo(origHost);
        if (res != 0) {
            return res;
        }

        res = otherEntry.origDb.compareTo(origDb);
        if (res != 0) {
            return res;
        }

        res = otherEntry.realOrigUser.compareTo(realOrigUser);
        if (res != 0) {
            return res;
        }

        return otherEntry.origTbl.compareTo(origTbl);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof TablePrivEntry)) {
            return false;
        }

        TablePrivEntry otherEntry = (TablePrivEntry) other;
        if (origHost.equals(otherEntry.origHost) && realOrigUser.equals(otherEntry.realOrigUser)
                && origDb.equals(otherEntry.origDb) && origTbl.equals(otherEntry.origTbl)
                && isDomain == otherEntry.isDomain) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("db priv. host: ").append(origHost).append(", db: ").append(origDb);
        sb.append(", user: ").append(realOrigUser).append(", tbl: ").append(origTbl);
        sb.append(", priv: ").append(privSet).append(", set by resolver: ").append(isSetByDomainResolver);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = TablePrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        super.write(out);

        Text.writeString(out, origTbl);

        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        origTbl = Text.readString(in);
    }

    @Override
    public String toGrantSQL() {
        GrantPrivilegeStmt stmt = new GrantPrivilegeStmt(null, "TABLE", getUserIdent(), false);
        stmt.setAnalysedTable(privSet, new TablePattern(origDb, origTbl));
        return AstToStringBuilder.toString(stmt);
    }

}
