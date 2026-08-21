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

package com.starrocks.catalog;

import com.google.common.collect.ImmutableSet;
import com.starrocks.persist.TextAnalyzerLog;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateTextAnalyzerStmt;
import com.starrocks.sql.ast.DropTextAnalyzerStmt;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.starrocks.analysis.InvertedIndexUtil.INVERTED_INDEX_ANALYZER_KEY;

public final class TextAnalyzerMgr {
    private static final Set<String> CREATE_PROPERTIES = ImmutableSet.of("definition");

    private TextAnalyzerMgr() {
    }

    public static void create(CreateTextAnalyzerStmt stmt, ConnectContext context) {
        ResolvedName name = resolveName(stmt.getAnalyzerName(), context, false);
        synchronized (name.db) {
            if (name.db.getTextAnalyzer(name.objectName) != null) {
                throw new SemanticException("TEXT ANALYZER " + stmt.getAnalyzerName() + " already exists");
            }
            Map<String, String> properties = stmt.getProperties();
            if (properties == null || !properties.keySet().equals(CREATE_PROPERTIES)
                    || StringUtils.isBlank(properties.get("definition"))) {
                throw new SemanticException("CREATE TEXT ANALYZER requires exactly one property: definition");
            }
            TextAnalyzer.Definition definition = TextAnalyzer.canonicalize(properties.get("definition"));
            checkClusterCapability();
            String owner = context.getCurrentUserIdentity() == null ? "" : context.getCurrentUserIdentity().toString();
            TextAnalyzer analyzer = new TextAnalyzer(GlobalStateMgr.getCurrentState().getNextId(), name.db.getId(),
                    name.objectName, definition.getCanonicalJson(), definition.getDigest(),
                    TextAnalyzer.RUNTIME_ABI_VERSION, System.currentTimeMillis(), owner);
            name.db.putTextAnalyzer(analyzer);
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logCreateTextAnalyzer(new TextAnalyzerLog(name.db.getId(), analyzer));
        }
    }

    public static void checkClusterCapability() {
        SystemInfoService clusterInfo = GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo();
        List<String> unsupported = new ArrayList<>();
        for (Long id : clusterInfo.getBackendIds(true)) {
            ComputeNode node = clusterInfo.getBackend(id);
            if (node != null && node.getTextAnalyzerRuntimeAbi() < TextAnalyzer.RUNTIME_ABI_VERSION) {
                unsupported.add(node.getHost() + ":" + node.getHeartbeatPort());
            }
        }
        for (Long id : clusterInfo.getComputeNodeIds(true)) {
            ComputeNode node = clusterInfo.getComputeNode(id);
            if (node != null && node.getTextAnalyzerRuntimeAbi() < TextAnalyzer.RUNTIME_ABI_VERSION) {
                unsupported.add(node.getHost() + ":" + node.getHeartbeatPort());
            }
        }
        if (!unsupported.isEmpty()) {
            throw new SemanticException("TEXT ANALYZER runtime ABI " + TextAnalyzer.RUNTIME_ABI_VERSION
                    + " is not supported by all alive BE/CN nodes: " + String.join(", ", unsupported));
        }
    }

    public static void drop(DropTextAnalyzerStmt stmt, ConnectContext context) {
        ResolvedName name = resolveName(stmt.getAnalyzerName(), context, false);
        synchronized (name.db) {
            TextAnalyzer analyzer = name.db.getTextAnalyzer(name.objectName);
            if (analyzer == null) {
                if (stmt.isIfExists()) {
                    return;
                }
                throw new SemanticException("Unknown TEXT ANALYZER " + stmt.getAnalyzerName());
            }
            List<String> references = findReferences(name.db, analyzer);
            if (!references.isEmpty()) {
                throw new SemanticException("Cannot drop TEXT ANALYZER " + stmt.getAnalyzerName()
                        + " because it is referenced by indexes: " + String.join(", ", references));
            }
            name.db.removeTextAnalyzer(name.objectName);
            GlobalStateMgr.getCurrentState().getEditLog()
                    .logDropTextAnalyzer(new TextAnalyzerLog(name.db.getId(), name.objectName));
        }
    }

    public static ResolvedName resolveName(String value, ConnectContext context, boolean allowDatabaseOnly) {
        String dbName;
        String objectName = null;
        String[] parts = value == null ? new String[0] : value.split("\\.", -1);
        if (allowDatabaseOnly) {
            dbName = value;
        } else if (parts.length == 1) {
            dbName = context.getDatabase();
            objectName = parts[0];
        } else if (parts.length == 2) {
            dbName = parts[0];
            objectName = parts[1];
        } else {
            throw new SemanticException("TEXT ANALYZER name must be [database.]name");
        }
        if (StringUtils.isBlank(dbName) || (!allowDatabaseOnly && StringUtils.isBlank(objectName))) {
            throw new SemanticException("No database selected for TEXT ANALYZER " + value);
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbName);
        if (db == null) {
            throw new SemanticException("Unknown database " + dbName);
        }
        return new ResolvedName(db, objectName);
    }

    public static TextAnalyzer require(String value, ConnectContext context) {
        ResolvedName name = resolveName(value, context, false);
        TextAnalyzer analyzer = name.db.getTextAnalyzer(name.objectName);
        if (analyzer == null) {
            throw new SemanticException("Unknown TEXT ANALYZER " + value);
        }
        return analyzer;
    }

    public static List<String> findReferences(Database db, TextAnalyzer analyzer) {
        List<Database> databases = new ArrayList<>();
        for (Long dbId : GlobalStateMgr.getCurrentState().getLocalMetastore().getDbIds()) {
            Database candidateDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(dbId);
            if (candidateDb != null) {
                databases.add(candidateDb);
            }
        }
        return findReferencesInDatabases(databases, db, analyzer);
    }

    static List<String> findReferencesInDatabases(List<Database> databases, Database db, TextAnalyzer analyzer) {
        String qualifiedName = db.getOriginName() + "." + analyzer.getName();
        List<String> references = new ArrayList<>();
        for (Database candidateDb : databases) {
            for (Table table : candidateDb.getTables()) {
                if (!(table instanceof OlapTable)) {
                    continue;
                }
                for (Index index : ((OlapTable) table).getIndexes()) {
                    Map<String, String> properties = index.getProperties();
                    if (properties != null
                            && qualifiedName.equalsIgnoreCase(properties.get(INVERTED_INDEX_ANALYZER_KEY))) {
                        references.add(candidateDb.getOriginName() + "." + table.getName()
                                + "." + index.getIndexName());
                    }
                }
            }
        }
        return references;
    }

    public static String toCreateSql(Database db, TextAnalyzer analyzer) {
        String escapedDefinition = analyzer.getCanonicalDefinition().replace("'", "''");
        return "CREATE TEXT ANALYZER `" + db.getOriginName() + "`.`" + analyzer.getName()
                + "` PROPERTIES (\"definition\" = '" + escapedDefinition + "')";
    }

    public static final class ResolvedName {
        private final Database db;
        private final String objectName;

        private ResolvedName(Database db, String objectName) {
            this.db = db;
            this.objectName = objectName;
        }

        public Database getDb() {
            return db;
        }

        public String getObjectName() {
            return objectName;
        }
    }
}
