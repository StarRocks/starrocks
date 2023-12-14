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

package com.starrocks.sql.analyzer;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.AstVisitor;
import com.starrocks.sql.ast.CancelRefreshDictionaryStmt;
import com.starrocks.sql.ast.CreateDictionaryStmt;
import com.starrocks.sql.ast.DropDictionaryStmt;
import com.starrocks.sql.ast.RefreshDictionaryStmt;
import com.starrocks.sql.ast.ShowDictionaryStmt;
import com.starrocks.sql.ast.StatementBase;

import java.util.List;

public class DictionaryAnalyzer {
    public static void analyze(StatementBase stmt, ConnectContext session) {
        new DictionaryAnalyzer.DictionaryAnalyzerVisitor().visit(stmt, session);
    }

    static class DictionaryAnalyzerVisitor extends AstVisitor<Void, ConnectContext> {
        @Override
        public Void visitCreateDictionaryStatement(CreateDictionaryStmt statement, ConnectContext context) {
            String dictionaryName = statement.getDictionaryName();
            if (GlobalStateMgr.getCurrentState().getDictionaryMgr().isExist(dictionaryName)) {
                throw new SemanticException("dictionary: " + dictionaryName + " is exist");
            }

            String queryableObject = statement.getQueryableObject();
            Database db = GlobalStateMgr.getCurrentState().getDb(context.getDatabase());
            if (db == null) {
                throw new SemanticException("USE a Database before CREATE DICTIONARY");
            }

            Table tbl = db.getTable(queryableObject);
            if (tbl == null) {
                throw new SemanticException(queryableObject + " does not exist");
            }

            // naive implementation with one key and one value
            // both a Int type
            List<String> dictionaryKeys = statement.getDictionaryKeys();
            List<String> dictionaryValues = statement.getDictionaryValues();

            if (dictionaryKeys.size() == 0) {
                throw new SemanticException("empty key list for dictionary: " + dictionaryName);
            }

            if (dictionaryValues.size() == 0) {
                throw new SemanticException("empty value list for dictionary: " + dictionaryName);
            }

            for (String key : dictionaryKeys) {
                for (String value : dictionaryValues) {
                    if (key.equals(value)) {
                        throw new SemanticException("column: " + key + " is both in keys and values");
                    }
                }
            }

            for (String key : dictionaryKeys) {
                if (tbl.getColumn(key) == null) {
                    throw new SemanticException("analyze dictionary keys failed, key: " + key + " not found");
                }
                Type t = tbl.getColumn(key).getType();
                if (!(t.isBoolean() || t.isNumericType() || t.isVarchar() || t.isDate() || t.isDatetime())) {
                    throw new SemanticException("dictionary key column[" + key + "] type not supported: " + t.toSql());
                }
            }

            for (String value : dictionaryValues) {
                if (tbl.getColumn(value) == null) {
                    throw new SemanticException("analyze dictionary values failed, value: " + value + " not found");
                }
                Type t = tbl.getColumn(value).getType();
                if (!(t.isBoolean() || t.isIntegerType() || t.isLargeint() || t.isVarchar() || t.isDate() ||
                        t.isDatetime())) {
                    throw new SemanticException("dictionary value column[" + value + "] type not supported: " + t.toSql());
                }
            }

            return null;
        }

        @Override
        public Void visitDropDictionaryStatement(DropDictionaryStmt statement, ConnectContext context) {
            String dictionaryName = statement.getDictionaryName();
            if (!GlobalStateMgr.getCurrentState().getDictionaryMgr().isExist(dictionaryName)) {
                throw new SemanticException("dictionary: " + dictionaryName + " does not exist");
            }
            return null;
        }

        @Override
        public Void visitRefreshDictionaryStatement(RefreshDictionaryStmt statement, ConnectContext context) {
            String dictionaryName = statement.getDictionaryName();
            if (!GlobalStateMgr.getCurrentState().getDictionaryMgr().isExist(dictionaryName)) {
                throw new SemanticException("dictionary: " + dictionaryName + " does not exist");
            }
            return null;
        }

        @Override
        public Void visitShowDictionaryStatement(ShowDictionaryStmt statement, ConnectContext context) {
            return null;
        }

        @Override
        public Void visitCancelRefreshDictionaryStatement(CancelRefreshDictionaryStmt statement, ConnectContext context) {
            String dictionaryName = statement.getDictionaryName();
            if (!GlobalStateMgr.getCurrentState().getDictionaryMgr().isExist(dictionaryName)) {
                throw new SemanticException("dictionary: " + dictionaryName + " does not exist");
            }
            return null;
        }
    }
}
