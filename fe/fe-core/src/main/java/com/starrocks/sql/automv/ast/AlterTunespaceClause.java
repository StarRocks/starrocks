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

package com.starrocks.sql.automv.ast;

import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Database;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.automv.qe.QueryStatementPlus;

import java.util.Collections;

public abstract class AlterTunespaceClause {
    public static final class AppendClause extends AlterTunespaceClause {
        private QueryStatementPlus queryStatement;

        public AppendClause(QueryStatement queryStatement) {
            this.queryStatement = QueryStatementPlus.of(queryStatement, Collections.emptyMap());
        }

        public QueryStatementPlus getQueryStatement() {
            return queryStatement;
        }

        public void setQueryStatement(QueryStatementPlus queryStatement) {
            this.queryStatement = queryStatement;
        }

    }

    public static final class PopulateFromLegacyMVClause extends AlterTunespaceClause {
        private QualifiedName dbName;
        private Database db;

        public PopulateFromLegacyMVClause(QualifiedName dbName) {
            this.dbName = dbName;
        }

        public QualifiedName getDbName() {
            return dbName;
        }

        public void setDbName(QualifiedName dbName) {
            this.dbName = dbName;
        }

        public Database getDb() {
            return db;
        }

        public void setDb(Database db) {
            this.db = db;
        }
    }

    public static final class PopulateFromTunespaceClause extends AlterTunespaceClause {
        private TableName srcTableName;

        public PopulateFromTunespaceClause(TableName srcTableName) {
            this.srcTableName = srcTableName;
        }

        public TableName getSrcTableName() {
            return srcTableName;
        }

        public void setSrcTableName(TableName srcTableName) {
            this.srcTableName = srcTableName;
        }
    }

    public static final class PopulateAsQueryClause extends AlterTunespaceClause {
        private QueryStatement queryStatement;

        public PopulateAsQueryClause(QueryStatement queryStatement) {
            this.queryStatement = queryStatement;
        }

        public QueryStatement getQueryStatement() {
            return queryStatement;
        }

        public void setQueryStatement(QueryStatement queryStatement) {
            this.queryStatement = queryStatement;
        }
    }
}
