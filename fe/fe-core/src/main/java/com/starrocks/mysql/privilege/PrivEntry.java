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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/PrivEntry.java

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

import com.google.gson.annotations.SerializedName;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.CaseSensibility;
import com.starrocks.common.FeMetaVersion;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.persist.gson.GsonPostProcessable;
import com.starrocks.persist.gson.GsonPreProcessable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public abstract class PrivEntry implements Comparable<PrivEntry>, Writable, GsonPostProcessable, GsonPreProcessable {
    protected static final String ANY_HOST = "%";
    protected static final String ANY_USER = "%";

    // host is not case sensitive
    protected transient PatternMatcher hostPattern;
    @SerializedName("origHost")
    protected String origHost;
    protected boolean isAnyHost = false;
    // user name is case sensitive
    protected transient PatternMatcher userPattern;
    @SerializedName("origUser")
    protected String origUserForJsonPersist;

    protected String realOrigUser;
    protected boolean isAnyUser = false;
    @SerializedName("privSet")
    protected PrivBitSet privSet;
    // true if this entry is set by domain resolver
    @SerializedName("isSetByDomainResolver")
    protected boolean isSetByDomainResolver = false;
    // true if origHost is a domain name.
    // For global priv entry, if isDomain is true, it should only be used for priv checking, not password checking
    @SerializedName("isDomain")
    protected boolean isDomain = false;

    // isClassNameWrote to guarantee the class name can only be written once when persisting.
    // see PrivEntry.read() for more details.
    protected boolean isClassNameWrote = false;

    private transient UserIdentity userIdentity;

    protected PrivEntry() {
    }

    protected PrivEntry(String origHost, String origUser, boolean isDomain, PrivBitSet privSet) {
        this.origHost = origHost;
        this.realOrigUser = origUser;
        this.isDomain = isDomain;
        this.privSet = privSet;
    }

    protected void analyse() throws AnalysisException {
        if (origHost.equals(ANY_HOST)) {
            isAnyHost = true;
        }
        this.hostPattern = PatternMatcher.createMysqlPattern(origHost, CaseSensibility.HOST.getCaseSensibility());

        if (realOrigUser.equals(ANY_USER)) {
            isAnyUser = true;
        }
        this.userPattern = PatternMatcher.createMysqlPattern(realOrigUser, CaseSensibility.USER.getCaseSensibility());
        if (isDomain) {
            userIdentity = UserIdentity.createAnalyzedUserIdentWithDomain(realOrigUser, origHost);
        } else {
            userIdentity = UserIdentity.createAnalyzedUserIdentWithIp(realOrigUser, origHost);
        }
    }

    public PatternMatcher getHostPattern() {
        return hostPattern;
    }

    public String getOrigHost() {
        return origHost;
    }

    public boolean isAnyHost() {
        return isAnyHost;
    }

    public PatternMatcher getUserPattern() {
        return userPattern;
    }

    public String getOrigUser() {
        return realOrigUser;
    }

    public boolean isAnyUser() {
        return isAnyUser;
    }

    public PrivBitSet getPrivSet() {
        return privSet;
    }

    public void setPrivSet(PrivBitSet privSet) {
        this.privSet = privSet;
    }

    public boolean isSetByDomainResolver() {
        return isSetByDomainResolver;
    }

    public void setSetByDomainResolver(boolean isSetByDomainResolver) {
        this.isSetByDomainResolver = isSetByDomainResolver;
    }

    public UserIdentity getUserIdent() {
        return userIdentity;
    }

    public abstract boolean keyMatch(PrivEntry other);

    /*
     * It's a bit complicated when persisting instance which its class has derived classes.
     * eg: A (top class) -> B (derived) -> C (derived)
     *
     * Write process:
     * C.write()
     *      |
     *      --- write class name
     *      |
     *      --- super.write()    -----> B.write()
     *      |                               |
     *      --- write C's self members      --- write class name (if not write before)
     *                                      |
     *                                      --- super.write()    -----> A.write()
     *                                      |                               |
     *                                      --- write B's self members      --- write class name (if not write before)
     *                                                                      |
     *                                                                      --- write A's self members
     *
     * So the final write order is:
     *      1. C's class name
     *      2. A's self members
     *      3. B's self members
     *      4. C's self members
     *
     * In case that class name should only be wrote once, we use isClassNameWrote flag.
     *
     * Read process:
     * static A.read()
     *      |
     *      --- read class name and instantiated the class instance (eg. C class)
     *      |
     *      --- C.readFields()
     *          |
     *          --- super.readFields() --> B.readFields()
     *          |                           |
     *          --- read C's self members   --- super.readFields() --> A.readFields()
     *                                      |                           |
     *                                      --- read B's self members   --- read A's self members
     *
     *  So the final read order is:
     *      1. C's class name
     *      2. A's self members
     *      3. B's self members
     *      4. C's self members
     *
     *  Which is same as Write order.
     */
    public static PrivEntry read(DataInput in) throws IOException {
        String className = Text.readString(in);
        if (className.startsWith("org.apache.doris")) {
            className = className.replaceFirst("org.apache.doris", "com.starrocks");
        }
        PrivEntry privEntry = null;
        try {
            Class<? extends PrivEntry> derivedClass = (Class<? extends PrivEntry>) Class.forName(className);
            privEntry = derivedClass.newInstance();
            Class[] paramTypes = {DataInput.class};
            Method readMethod = derivedClass.getMethod("readFields", paramTypes);
            Object[] params = {in};
            readMethod.invoke(privEntry, params);

            return privEntry;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                 | SecurityException | IllegalArgumentException | InvocationTargetException e) {
            throw new IOException("failed read PrivEntry", e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = PrivEntry.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        Text.writeString(out, origHost);
        Text.writeString(out, ClusterNamespace.getFullName(realOrigUser));
        privSet.write(out);

        out.writeBoolean(isSetByDomainResolver);
        out.writeBoolean(isDomain);

        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        origHost = Text.readString(in);
        realOrigUser = ClusterNamespace.getNameFromFullName(Text.readString(in));
        privSet = PrivBitSet.read(in);
        isSetByDomainResolver = in.readBoolean();
        if (GlobalStateMgr.getCurrentStateJournalVersion() >= FeMetaVersion.VERSION_69) {
            isDomain = in.readBoolean();
        }
    }

    @Override
    public void gsonPostProcess() throws IOException {
        realOrigUser = ClusterNamespace.getNameFromFullName(origUserForJsonPersist);
    }

    @Override
    public void gsonPreProcess() throws IOException {
        origUserForJsonPersist = ClusterNamespace.getFullName(realOrigUser);
    }

    @Override
    public int compareTo(PrivEntry o) {
        throw new NotImplementedException();
    }

    /**
     * used for `SHOW GRANTS FOR`
     */
    public String toGrantSQL() {
        throw new NotImplementedException();
    }
}
