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

package com.starrocks.datacache;

import com.starrocks.analysis.Expr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.QualifiedName;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class DataCacheMgr {

    private static final DataCacheMgr INSTANCE = new DataCacheMgr();

    private final AtomicLong ids = new AtomicLong();
    private final ReadWriteLock dataCacheMgrLock = new ReentrantReadWriteLock();
    private final Map<Long, DataCacheRule> idToCacheRuleMap = new HashMap<>();
    private final CatalogMapping catalogMapping = new CatalogMapping();
    private static final String STAR_MATCH_ALL = "*";

    public static DataCacheMgr getInstance() {
        return INSTANCE;
    }

    public void clearRules() {
        writeLock();
        try {
            ids.set(0);
            idToCacheRuleMap.clear();
            catalogMapping.clear();
        } finally {
            writeUnLock();
        }
    }

    public void createCacheRule(QualifiedName target, Expr predicates, int priority,
                                             Map<String, String> properties) throws SemanticException {
        writeLock();

        try {
            long id  = ids.getAndIncrement();
            DataCacheRule cacheRule = new DataCacheRule(id, target, predicates, priority, properties);
            idToCacheRuleMap.put(id, cacheRule);

            String catalogName = target.getParts().get(0);
            String dbName = target.getParts().get(1);
            String tblName = target.getParts().get(2);

            // Put in catalog level
            DbMapping dbMapping = catalogMapping.get(catalogName);
            if (dbMapping == null) {
                dbMapping = new DbMapping();
                catalogMapping.put(catalogName, dbMapping);
            }

            // Put in db level
            TblMapping tblMapping = dbMapping.get(dbName);
            if (tblMapping == null) {
                tblMapping = new TblMapping();
                dbMapping.put(dbName, tblMapping);
            }

            tblMapping.put(tblName, cacheRule);
        } finally {
            writeUnLock();
        }
    }

    public void dropCacheRule(long id) {
        writeLock();

        try {
            DataCacheRule cacheRule = idToCacheRuleMap.remove(id);
            List<String> parts = cacheRule.getTarget().getParts();
            String catalogName = parts.get(0);
            String dbName = parts.get(1);
            String tblName = parts.get(2);
            catalogMapping.get(catalogName).get(dbName).remove(tblName);
        } finally {
            writeUnLock();
        }
    }

    public boolean isExistCacheRule(long id) {
        readLock();

        try {
            return idToCacheRuleMap.containsKey(id);
        } finally {
            readUnlock();
        }
    }

    public Optional<DataCacheRule> getCacheRule(String catalogName, String dbName, String tblName) {
        readLock();

        try {
            // check in catalog level
            DbMapping dbMapping;
            if ((dbMapping = catalogMapping.get(STAR_MATCH_ALL)) == null) {
                dbMapping = catalogMapping.get(catalogName);
            }
            if (dbMapping == null) {
                return Optional.empty();
            }

            // check in db level
            TblMapping tblMapping;
            if ((tblMapping = dbMapping.get(STAR_MATCH_ALL)) == null) {
                tblMapping = dbMapping.get(dbName);
            }
            if (tblMapping == null) {
                return Optional.empty();
            }

            // check in tbl level
            DataCacheRule dataCacheRule;
            if ((dataCacheRule = tblMapping.get(STAR_MATCH_ALL)) == null) {
                dataCacheRule = tblMapping.get(tblName);
            }
            return Optional.ofNullable(dataCacheRule);
        } finally {
            readUnlock();
        }
    }

    public Optional<DataCacheRule> getCacheRule(QualifiedName qualifiedName) {
        List<String> parts = qualifiedName.getParts();
        return getCacheRule(parts.get(0), parts.get(1), parts.get(2));
    }

    public void throwExceptionIfRuleIsConflicted(String otherCatalog, String otherDb, String otherTbl) {
        readLock();

        try {
            for (Map.Entry<Long, DataCacheRule> entry : idToCacheRuleMap.entrySet()) {
                List<String> parts = entry.getValue().getTarget().getParts();
                String catalog = parts.get(0);
                if (isMatchAll(catalog) || isMatchAll(otherCatalog)) {
                    throw new SemanticException(String.format("DataCache rule target's catalog name: %s is " +
                            "conflict with existed rule: %s", otherCatalog, entry.getValue()));
                }

                if (!catalog.equals(otherCatalog)) {
                    continue;
                }

                String db = parts.get(1);
                if (isMatchAll(db) || isMatchAll(otherDb)) {
                    throw new SemanticException(String.format("DataCache rule target's database name: %s is " +
                            "conflict with existed rule %s", otherDb, entry.getValue()));
                }

                if (!db.equals(otherDb)) {
                    continue;
                }

                String tbl = parts.get(2);
                if (isMatchAll(tbl) || isMatchAll(otherTbl) || tbl.equals(otherTbl)) {
                    throw new SemanticException(String.format("DataCache rule target's table name: %s " +
                            "is conflict with existed rule %s", otherTbl, entry.getValue()));
                }
            }
        } finally {
            readUnlock();
        }
    }

    public List<List<String>> getShowResultSetRows() {
        readLock();

        try {
            List<DataCacheRule> dataCacheRules = new ArrayList<>(idToCacheRuleMap.size());
            for (Map.Entry<Long, DataCacheRule> entry : idToCacheRuleMap.entrySet()) {
                dataCacheRules.add(entry.getValue());
            }

            // Sort by id ascended
            dataCacheRules.sort(Comparator.comparingInt(o -> (int) o.getId()));

            List<List<String>> result = new ArrayList<>(dataCacheRules.size());
            for (DataCacheRule rule : dataCacheRules) {
                result.add(rule.getShowResultSetRows());
            }
            return result;
        } finally {
            readUnlock();
        }
    }

    private boolean isMatchAll(String pattern) {
        return pattern.equals(STAR_MATCH_ALL);
    }

    private void readLock() {
        this.dataCacheMgrLock.readLock().lock();
    }

    private void readUnlock() {
        this.dataCacheMgrLock.readLock().unlock();
    }

    private void writeLock() {
        this.dataCacheMgrLock.writeLock().lock();
    }

    private void writeUnLock() {
        this.dataCacheMgrLock.writeLock().unlock();
    }

    static class CatalogMapping {
        private final Map<String, DbMapping> mapping = new HashMap<>();
        protected DbMapping get(String catalogName) {
            return mapping.get(catalogName);
        }
        protected void put(String catalogName, DbMapping dbMapping) {
            mapping.put(catalogName, dbMapping);
        }
        protected void clear() {
            mapping.clear();
        }
    }

    static class DbMapping {
        private final Map<String, TblMapping> mapping = new HashMap<>();
        protected TblMapping get(String dbName) {
            return mapping.get(dbName);
        }
        protected void put(String dbName, TblMapping tblMapping) {
            mapping.put(dbName, tblMapping);
        }
    }

    static class TblMapping {
        private final Map<String, DataCacheRule> mapping = new HashMap<>();
        protected DataCacheRule get(String tblName) {
            return mapping.get(tblName);
        }

        protected void put(String tblName, DataCacheRule dataCacheRule) {
            mapping.put(tblName, dataCacheRule);
        }

        protected void remove(String tblName) {
            mapping.remove(tblName);
        }
    }
}
