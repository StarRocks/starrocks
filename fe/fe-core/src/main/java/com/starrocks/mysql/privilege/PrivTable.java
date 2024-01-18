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
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/mysql/privilege/PrivTable.java

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

import com.google.common.base.Preconditions;
import com.starrocks.common.error.ErrorCode;
import com.starrocks.common.error.ErrorReport;
import com.starrocks.common.exception.AnalysisException;
import com.starrocks.common.exception.DdlException;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.sql.ast.UserIdentity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public abstract class PrivTable implements Writable {
    private static final Logger LOG = LogManager.getLogger(PrivTable.class);

    // keep user identity sorted
    protected Map<UserIdentity, List<PrivEntry>> map = new TreeMap<>(new Comparator<UserIdentity>() {
        @Override
        public int compare(UserIdentity o1, UserIdentity o2) {
            int compareByHost = o2.getHost().compareTo(o1.getHost());
            if (compareByHost != 0) {
                return compareByHost;
            }
            return o2.getUser().compareTo(o1.getUser());
        }
    });

    // see PrivEntry for more detail
    protected boolean isClassNameWrote = false;

    /*
     * Add an entry to priv table.
     * If entry already exists and errOnExist is false, we try to reset or merge the new priv entry with existing one.
     * NOTICE, this method does not set password for the newly added entry if this is a user priv table, the caller
     * need to set password later.
     */
    public PrivEntry addEntry(PrivEntry newEntry, boolean errOnExist, boolean errOnNonExist) throws DdlException {
        PrivEntry existingEntry = getExistingEntry(newEntry);
        if (existingEntry == null) {
            if (errOnNonExist) {
                throw new DdlException("User " + newEntry.getUserIdent() + " does not exist");
            }
            UserIdentity newUser = newEntry.getUserIdent();
            List<PrivEntry> entries = map.computeIfAbsent(newUser, k -> new ArrayList<>());
            entries.add(newEntry);
            LOG.debug("add priv entry: {}", newEntry);
            return newEntry;
        } else {
            if (errOnExist) {
                throw new DdlException("User already exist");
            } else {
                checkOperationAllowed(existingEntry, newEntry, "ADD ENTRY");
                // if existing entry is set by domain resolver, just replace it with the new entry.
                // if existing entry is not set by domain resolver, merge the 2 entries.
                if (existingEntry.isSetByDomainResolver()) {
                    existingEntry.setPrivSet(newEntry.getPrivSet());
                    existingEntry.setSetByDomainResolver(newEntry.isSetByDomainResolver());
                    LOG.debug("reset priv entry: {}", existingEntry);
                } else if (!newEntry.isSetByDomainResolver()) {
                    mergePriv(existingEntry, newEntry);
                    existingEntry.setSetByDomainResolver(false);
                    LOG.debug("merge priv entry: {}", existingEntry);
                }
            }
            return existingEntry;
        }
    }

    public void dropEntry(PrivEntry entry) {
        UserIdentity userIdentity = entry.getUserIdent();
        List<PrivEntry> userPrivEntryList = map.get(userIdentity);
        if (userPrivEntryList == null) {
            return;
        }
        Iterator<PrivEntry> iter = userPrivEntryList.iterator();
        while (iter.hasNext()) {
            PrivEntry privEntry = iter.next();
            if (privEntry.keyMatch(entry)) {
                iter.remove();
                LOG.info("drop priv entry: {}", privEntry);
                break;
            }
        }
        if (userPrivEntryList.isEmpty()) {
            map.remove(userIdentity);
        }
    }

    public void clearEntriesSetByResolver() {
        Iterator<Map.Entry<UserIdentity, List<PrivEntry>>> mapIter = map.entrySet().iterator();
        while (mapIter.hasNext()) {
            Map.Entry<UserIdentity, List<PrivEntry>> entry = mapIter.next();
            Iterator<PrivEntry> iter = entry.getValue().iterator();
            while (iter.hasNext()) {
                PrivEntry privEntry = iter.next();
                if (privEntry.isSetByDomainResolver()) {
                    iter.remove();
                    LOG.info("drop priv entry set by resolver: {}", privEntry);
                }
            }
            if (entry.getValue().isEmpty()) {
                mapIter.remove();
            }
        }
    }

    // drop all entries which user name are matched, and is not set by resolver
    public void dropUser(UserIdentity userIdentity) {
        map.remove(userIdentity);
    }

    public void revoke(PrivEntry entry, boolean errOnNonExist, boolean deleteEntryWhenEmpty) throws DdlException {
        PrivEntry existingEntry = getExistingEntry(entry);
        if (existingEntry == null) {
            if (errOnNonExist) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT, entry.getOrigUser(),
                        entry.getOrigHost());
            }
            return;
        }

        checkOperationAllowed(existingEntry, entry, "REVOKE");

        // check if privs to be revoked exist in priv entry.
        PrivBitSet tmp = existingEntry.getPrivSet().copy();
        tmp.and(entry.getPrivSet());
        if (tmp.isEmpty()) {
            if (errOnNonExist) {
                ErrorReport.reportDdlException(ErrorCode.ERR_NONEXISTING_GRANT, entry.getOrigUser(),
                        entry.getOrigHost());
            }
            // there is no such priv, nothing need to be done
            return;
        }

        // revoke privs from existing priv entry
        LOG.debug("before revoke: {}, privs to be revoked: {}",
                existingEntry.getPrivSet(), entry.getPrivSet());
        tmp = existingEntry.getPrivSet().copy();
        tmp.xor(entry.getPrivSet());
        existingEntry.getPrivSet().and(tmp);
        LOG.debug("after revoke: {}", existingEntry);

        if (existingEntry.getPrivSet().isEmpty() && deleteEntryWhenEmpty) {
            // no priv exists in this entry, remove it
            dropEntry(existingEntry);
        }
    }

    /*
     * the priv entry is classified by 'set by domain resolver'
     * or 'NOT set by domain resolver'(other specified operations).
     * if the existing entry is set by resolver, it can be reset by resolver or set by specified ops.
     * in other word, if the existing entry is NOT set by resolver, it can not be set by resolver.
     */
    protected void checkOperationAllowed(PrivEntry existingEntry, PrivEntry newEntry, String op) throws DdlException {
        if (!existingEntry.isSetByDomainResolver() && newEntry.isSetByDomainResolver()) {
            throw new DdlException("the existing entry is NOT set by resolver: " + existingEntry + ","
                    + " can not be set by resolver " + newEntry + ", op: " + op);
        }
    }

    // Get existing entry which is the keys match the given entry
    protected PrivEntry getExistingEntry(PrivEntry entry) {
        List<PrivEntry> userPrivEntryList = map.get(entry.getUserIdent());
        if (userPrivEntryList == null) {
            return null;
        }
        for (PrivEntry existingEntry : userPrivEntryList) {
            if (existingEntry.keyMatch(entry)) {
                return existingEntry;
            }
        }
        return null;
    }

    private void mergePriv(PrivEntry first, PrivEntry second) {
        first.getPrivSet().or(second.getPrivSet());
        first.setSetByDomainResolver(first.isSetByDomainResolver() || second.isSetByDomainResolver());
    }

    public boolean doesUsernameExist(String qualifiedUsername) {
        for (UserIdentity userIdentity : map.keySet()) {
            if (userIdentity.getUser().equals(qualifiedUsername)) {
                return true;
            }
        }
        return false;
    }

    // for test only
    public void clear() {
        map.clear();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public static PrivTable read(DataInput in) throws IOException {
        String className = Text.readString(in);
        if (className.startsWith("org.apache.doris")) {
            className = className.replaceFirst("org.apache.doris", "com.starrocks");
        }
        PrivTable privTable = null;
        try {
            Class<? extends PrivTable> derivedClass = (Class<? extends PrivTable>) Class.forName(className);
            privTable = derivedClass.newInstance();
            Class[] paramTypes = {DataInput.class};
            Method readMethod = derivedClass.getMethod("readFields", paramTypes);
            Object[] params = {in};
            readMethod.invoke(privTable, params);

            return privTable;
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | NoSuchMethodException
                 | SecurityException | IllegalArgumentException | InvocationTargetException e) {
            throw new IOException("failed read PrivTable", e);
        }
    }

    public int size() {
        int sum = 0;
        for (Map.Entry<UserIdentity, List<PrivEntry>> entry : map.entrySet()) {
            sum += entry.getValue().size();
        }
        return sum;
    }

    /**
     * return a iterator used for a complete loop in the whole table
     * This is READ ONLY, please don't use it for any kinds of modification
     */
    public Iterator<PrivEntry> getFullReadOnlyIterator() {
        return new Iterator<PrivEntry>() {
            private Iterator<Map.Entry<UserIdentity, List<PrivEntry>>> mapIterator;
            private Iterator<PrivEntry> privEntryIterator;

            {
                // init
                mapIterator = map.entrySet().iterator();
                privEntryIterator = null;
            }

            @Override
            public boolean hasNext() {
                return mapIterator.hasNext() || privEntryIterator != null && privEntryIterator.hasNext();
            }

            @Override
            public PrivEntry next() {
                if (privEntryIterator == null || !privEntryIterator.hasNext()) {
                    if (!mapIterator.hasNext()) {
                        return null;
                    }
                    Map.Entry<UserIdentity, List<PrivEntry>> next = mapIterator.next();
                    privEntryIterator = next.getValue().iterator();
                }
                return privEntryIterator.next();
            }
        };
    }

    /**
     * return a iterator to all the entries that match currentUser
     */
    public Iterator<PrivEntry> getReadOnlyIteratorByUser(UserIdentity currentUser) {
        return getReadOnlyIteratorByUser(currentUser.getUser(), currentUser.getHost());
    }

    /**
     * return a iterator to all the entries that match user@host
     */
    public Iterator<PrivEntry> getReadOnlyIteratorByUser(String user, String host) {
        return new Iterator<PrivEntry>() {
            private Iterator<Map.Entry<UserIdentity, List<PrivEntry>>> mapIterator;
            // always point at the entry to be visit next
            private Iterator<PrivEntry> privEntryIterator;

            {
                // init
                mapIterator = map.entrySet().iterator();
                privEntryIterator = null;
                iterMapToNextMatchedIdentity();
            }

            /**
             * iterator to the next user identity that match user
             * return false if no such user found, true if found
             */
            private boolean iterMapToNextMatchedIdentity() {
                while (mapIterator.hasNext()) {
                    Map.Entry<UserIdentity, List<PrivEntry>> mapEntry = mapIterator.next();
                    List<PrivEntry> entries = mapEntry.getValue();
                    Preconditions.checkArgument(entries.size() > 0);
                    PrivEntry privEntry = entries.get(0);

                    // check user
                    if (!privEntry.isAnyUser() && !privEntry.getUserPattern().match(user)) {
                        continue;
                    }
                    // check host
                    if (!privEntry.isAnyHost() && !privEntry.getHostPattern().match(host)) {
                        continue;
                    }

                    // user & host all match
                    privEntryIterator = entries.iterator();
                    return true;
                }
                return false;
            }

            @Override
            public boolean hasNext() {
                if (privEntryIterator == null) {
                    return false;
                }
                if (privEntryIterator.hasNext()) {
                    return true;
                } else {
                    return iterMapToNextMatchedIdentity();
                }
            }

            @Override
            public PrivEntry next() {
                return privEntryIterator.next();
            }
        };
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\n");
        Iterator<PrivEntry> iter = this.getFullReadOnlyIterator();
        while (iter.hasNext()) {
            sb.append(iter.next()).append("\n");
        }
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (!isClassNameWrote) {
            String className = PrivTable.class.getCanonicalName();
            Text.writeString(out, className);
            isClassNameWrote = true;
        }
        out.writeInt(this.size());
        Iterator<PrivEntry> iter = this.getFullReadOnlyIterator();
        while (iter.hasNext()) {
            iter.next().write(out);
        }
        isClassNameWrote = false;
    }

    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            PrivEntry entry = PrivEntry.read(in);
            try {
                entry.analyse();
            } catch (AnalysisException e) {
                throw new IOException(e);
            }
            UserIdentity newUser = entry.getUserIdent();
            List<PrivEntry> entries = map.computeIfAbsent(newUser, k -> new ArrayList<>());
            entries.add(entry);
        }
    }

}
