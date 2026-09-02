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
import com.google.common.collect.ImmutableList;
import com.staros.util.LockCloseable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.ErrorCode;
import com.starrocks.common.ErrorReport;
import com.starrocks.metric.MetricRepo;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.SqlBlackListPersistInfo;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
<<<<<<< HEAD
import java.util.stream.Collectors;
=======
import java.util.regex.PatternSyntaxException;
>>>>>>> 4369e2b ([BugFix] Make SQL blacklist verification lock-free (#78476))

// Used by sql's blacklist
public class SqlBlackList {

    private static final Logger LOG = LogManager.getLogger(SqlBlackList.class);

    public void verifying(String sql) throws AnalysisException {
        String formatSql = sql.replace("\r", " ").replace("\n", " ").replaceAll("\\s+", " ");
        for (BlackListSql patternAndId : ruleSnapshot) {
            Matcher m = patternAndId.pattern.matcher(formatSql);
            if (m.find()) {
                MetricRepo.COUNTER_SQL_BLOCK_HIT_COUNT.increase(1L);
                ErrorReport.reportSqlBlackListException(ErrorCode.ERR_SQL_IN_BLACKLIST_ERROR, patternAndId.id);
            }
        }
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        try (LockCloseable ignored = new LockCloseable(updateLock)) {
            int cnt = reader.readInt();
            for (int i = 0; i < cnt; i++) {
                SqlBlackListPersistInfo sqlBlackListPersistInfo = reader.readJson(SqlBlackListPersistInfo.class);
                put(sqlBlackListPersistInfo.id, Pattern.compile(sqlBlackListPersistInfo.pattern));
            }
            LOG.info("loaded {} SQL blacklist patterns", sqlBlackListMap.size());
        }
    }

    // we use string of sql as key, and (pattern, id) as value.
    public long put(Pattern pattern) {
        try (LockCloseable ignored = new LockCloseable(updateLock)) {
            BlackListSql blackListSql = sqlBlackListMap.get(pattern.toString());
            if (blackListSql == null) {
                long id = ids.getAndIncrement();
<<<<<<< HEAD
                sqlBlackListMap.put(pattern.toString(), new BlackListSql(pattern, id));
=======
                GlobalStateMgr.getCurrentState().getEditLog().logAddSQLBlackList(
                        new SqlBlackListPersistInfo(id, pattern.pattern()),
                        wal -> {
                            sqlBlackListMap.put(pattern.toString(), new BlackListSql(pattern, id));
                            refreshSnapshot();
                        });
>>>>>>> 4369e2b ([BugFix] Make SQL blacklist verification lock-free (#78476))
                return id;
            } else {
                return blackListSql.id;
            }
        }
    }

    public void put(long id, Pattern pattern) {
        try (LockCloseable ignored = new LockCloseable(updateLock)) {
            BlackListSql blackListSql = sqlBlackListMap.get(pattern.toString());
            if (blackListSql == null) {
                ids.set(Math.max(ids.get(), id + 1));
                sqlBlackListMap.put(pattern.toString(), new BlackListSql(pattern, id));
                refreshSnapshot();
            }
        }
    }

    // we delete sql's regular expression use id, so we iterate this map.
    public void delete(long id) {
        try (LockCloseable ignored = new LockCloseable(updateLock)) {
            for (Map.Entry<String, BlackListSql> entry : sqlBlackListMap.entrySet()) {
                if (entry.getValue().id == id) {
                    sqlBlackListMap.remove(entry.getKey());
<<<<<<< HEAD
=======
                    refreshSnapshot();
                    return;
>>>>>>> 4369e2b ([BugFix] Make SQL blacklist verification lock-free (#78476))
                }
            }
        }
    }

    public void delete(List<Long> ids) {
        for (Long id : ids) {
            this.delete(id);
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        try (LockCloseable ignored = new LockCloseable(updateLock)) {
            // one for self and N for patterns
            final int cnt = 1 + sqlBlackListMap.size();
            SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.BLACKLIST_MGR, cnt);

            // write patterns
            writer.writeInt(sqlBlackListMap.size());
            for (BlackListSql p : sqlBlackListMap.values()) {
                writer.writeJson(new SqlBlackListPersistInfo(p.id, p.pattern.pattern()));
            }
            writer.close();
        }
    }

    public List<BlackListSql> getBlackLists() {
        return ruleSnapshot;
    }

    private void refreshSnapshot() {
        ruleSnapshot = this.sqlBlackListMap.values().stream().sorted(Comparator.comparing(x -> x.id))
                .collect(ImmutableList.toImmutableList());
    }

    @VisibleForTesting
    ReentrantLock getUpdateLock() {
        return updateLock;
    }

    private final ReentrantLock updateLock = new ReentrantLock();

    private volatile List<BlackListSql> ruleSnapshot = ImmutableList.of();

    // sqlBlackListMap: key is String(sql), value is BlackListSql.
    // BlackListSql is (Pattern, id). Pattern is the regular expression, id marks this sql, and is show with "show sqlblacklist";
    private final ConcurrentMap<String, BlackListSql> sqlBlackListMap = new ConcurrentHashMap<>();

    // ids used in sql blacklist
    private final AtomicLong ids = new AtomicLong();
<<<<<<< HEAD
=======

    protected void cleanup() {
        try (LockCloseable ignored = new LockCloseable(updateLock)) {
            sqlBlackListMap.clear();
            ids.set(0);
            refreshSnapshot();
        }
    }
>>>>>>> 4369e2b ([BugFix] Make SQL blacklist verification lock-free (#78476))
}
