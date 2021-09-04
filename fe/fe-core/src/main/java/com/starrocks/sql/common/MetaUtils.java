// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
package com.starrocks.sql.common;

import com.google.common.base.Strings;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.cluster.ClusterNamespace;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;

public class MetaUtils {
    public static Database getStarRocks(ConnectContext session, TableName tableName) {
        Database db = session.getCatalog().getDb(tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not find", tableName.getDb());
        }
        return db;
    }

    public static Table getStarRocksTable(ConnectContext session, TableName tableName) {
        Database db = session.getCatalog().getDb(tableName.getDb());
        if (db == null) {
            throw new SemanticException("Database %s is not find", tableName.getDb());
        }
        Table table = db.getTable(tableName.getTbl());
        if (table == null) {
            throw new SemanticException("Unknown table '%s", tableName.getTbl());
        }
        return table;
    }

    public static void normalizationTableName(ConnectContext connectContext, TableName tableName) {
        if (Strings.isNullOrEmpty(tableName.getDb())) {
            if (Strings.isNullOrEmpty(connectContext.getDatabase())) {
                throw new SemanticException("No database selected");
            }
            tableName.setDb(connectContext.getDatabase());
        } else {
            if (Strings.isNullOrEmpty(connectContext.getClusterName())) {
                throw new SemanticException("No cluster name");
            }
            tableName.setDb(ClusterNamespace.getFullName(connectContext.getClusterName(), tableName.getDb()));
        }

        if (Strings.isNullOrEmpty(tableName.getTbl())) {
            throw new SemanticException("Table name is null");
        }
    }
}
