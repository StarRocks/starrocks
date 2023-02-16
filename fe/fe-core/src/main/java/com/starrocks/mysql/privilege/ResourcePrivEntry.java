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

import com.starrocks.analysis.ResourcePattern;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.io.Text;
import com.starrocks.sql.analyzer.AstToStringBuilder;
import com.starrocks.sql.ast.GrantPrivilegeStmt;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ResourcePrivEntry extends PrivEntry {
    protected static final String ANY_RESOURCE = "*";

    protected PatternMatcher resourcePattern;
    protected String origResource;
    protected boolean isAnyResource;

    protected ResourcePrivEntry() {
    }

    protected ResourcePrivEntry(String origHost, String user, boolean isDomain, PrivBitSet privSet, String origResource) {
        super(origHost, user, isDomain, privSet);
        this.origResource = origResource;
    }

    @Override
    protected void analyse() throws AnalysisException {
        super.analyse();

        resourcePattern = PatternMatcher.createMysqlPattern(origResource.equals(ANY_RESOURCE) ? "%" : origResource,
                CaseSensibility.RESOURCE.getCaseSensibility());
        if (origResource.equals(ANY_RESOURCE)) {
            isAnyResource = true;
        }
    }

    public static ResourcePrivEntry create(String host, String resourceName, String user, boolean isDomain,
                                           PrivBitSet privs)
            throws AnalysisException {

        ResourcePrivEntry resourcePrivEntry = new ResourcePrivEntry(host, user, isDomain, privs, resourceName);
        resourcePrivEntry.analyse();
        return resourcePrivEntry;
    }

    public PatternMatcher getResourcePattern() {
        return resourcePattern;
    }

    public String getOrigResource() {
        return origResource;
    }

    @Override
    public int compareTo(PrivEntry other) {
        if (!(other instanceof ResourcePrivEntry)) {
            throw new ClassCastException("cannot cast " + other.getClass().toString() + " to " + this.getClass());
        }

        ResourcePrivEntry otherEntry = (ResourcePrivEntry) other;
        int res = otherEntry.origHost.compareTo(origHost);
        if (res != 0) {
            return res;
        }

        res = otherEntry.origResource.compareTo(origResource);
        if (res != 0) {
            return res;
        }

        return otherEntry.realOrigUser.compareTo(realOrigUser);
    }

    @Override
    public boolean keyMatch(PrivEntry other) {
        if (!(other instanceof ResourcePrivEntry)) {
            return false;
        }

        ResourcePrivEntry otherEntry = (ResourcePrivEntry) other;
        if (origHost.equals(otherEntry.origHost) && realOrigUser.equals(otherEntry.realOrigUser)
                && origResource.equals(otherEntry.origResource) && isDomain == otherEntry.isDomain) {
            return true;
        }
        return false;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("resource priv. host: ").append(origHost).append(", resource: ").append(origResource);
        sb.append(", user: ").append(realOrigUser);
        sb.append(", priv: ").append(privSet).append(", set by resolver: ").append(isSetByDomainResolver);
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = ResourcePrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        super.write(out);
        Text.writeString(out, origResource);
        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        origResource = Text.readString(in);
    }

    @Override
    public String toGrantSQL() {
        GrantPrivilegeStmt stmt = new GrantPrivilegeStmt(null, "RESOURCE", getUserIdent(), false);
        stmt.setAnalysedResource(privSet, new ResourcePattern(origResource));
        return AstToStringBuilder.toString(stmt);
    }
}
