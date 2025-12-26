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

package com.starrocks.meta;

import com.google.common.annotations.VisibleForTesting;
import com.staros.util.LockCloseable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.DeleteSqlDigestBlackLists;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.SqlDigestBlackListPersistInfo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SqlDigestBlackList {
    private static final Logger LOG = LogManager.getLogger(SqlDigestBlackList.class);

    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

    private final Set<String> sqlDigestBlackList = ConcurrentHashMap.newKeySet();

    public Set<String> getDigests() {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            return new TreeSet<>(sqlDigestBlackList);
        }
    }

    public void verifying(String digest) throws AnalysisException {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            if (sqlDigestBlackList.contains(digest)) {
                MetricRepo.COUNTER_SQL_BLOCK_HIT_COUNT.increase(1L);
                ErrorReport.reportAnalysisException(
                        ErrorCode.ERR_SQL_IN_BLACKLIST_ERROR.formatErrorMsg() + ". Digest: %s",
                        ErrorCode.ERR_SQL_IN_BLACKLIST_ERROR,
                        digest);
            }
        }
    }

    public void addDigest(String digest) {
        if (digest == null || digest.isEmpty()) {
            return;
        }
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logAddSqlDigestBlackList(new SqlDigestBlackListPersistInfo(digest),
                            wal -> sqlDigestBlackList.add(digest));
        }
    }

    public void put(String digest) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            sqlDigestBlackList.add(digest);
        }
    }

    public void deleteDigests(List<String> digests) {
        GlobalStateMgr.getCurrentState().getEditLog()
                .logDeleteSqlDigestBlackList(new DeleteSqlDigestBlackLists(digests), wal -> deleteAll(digests));
    }

    public void deleteAll(List<String> digests) {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            digests.forEach(sqlDigestBlackList::remove);
        }
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            int cnt = reader.readInt();
            for (int i = 0; i < cnt; i++) {
                String digest = reader.readString();
                sqlDigestBlackList.add(digest);
            }
            LOG.info("loaded {} SQL digest blacklist", sqlDigestBlackList.size());
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        try (LockCloseable ignored = new LockCloseable(rwLock.readLock())) {
            // one for self and N for digests
            final int cnt = 1 + sqlDigestBlackList.size();
            SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.DIGEST_BLACKLIST_MGR, cnt);

            writer.writeInt(sqlDigestBlackList.size());
            for (String digest : sqlDigestBlackList) {
                writer.writeString(digest);
            }
            writer.close();
        }
    }

    @VisibleForTesting
    protected void cleanup() {
        try (LockCloseable ignored = new LockCloseable(rwLock.writeLock())) {
            sqlDigestBlackList.clear();
        }
    }
}

