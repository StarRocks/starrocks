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

package com.starrocks.sql.optimizer.dump;

import com.google.common.collect.Maps;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.SubfieldExpr;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.View;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.Pair;
import com.starrocks.sql.ast.AstTraverser;
import com.starrocks.sql.ast.CTERelation;
import com.starrocks.sql.ast.JoinRelation;
import com.starrocks.sql.ast.Relation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.SetOperationRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.SubqueryRelation;
import com.starrocks.sql.ast.ViewRelation;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

public class DesensitizedInfoCollector {

    private final Map<String, String> desensitizedDict = Maps.newHashMap();

    private final QueryDumpInfo dumpInfo;

    private IdGenerator<DesensitizedId> idGenerator = DesensitizedId.createGenerator();

    public DesensitizedInfoCollector(QueryDumpInfo dumpInfo) {
        this.dumpInfo = dumpInfo;
    }

    public Map<String, String> getDesensitizedDict() {
        return desensitizedDict;
    }

    public void init() {
        fillBuiltInWords();

        for (Pair<String, Table> pair : dumpInfo.getTableMap().values()) {
            // desensitized dbName
            addIntoDict(pair.first);
            // desensitized tableName
            Table table = pair.second;
            addIntoDict(table.getName());
            for (Column column : table.getColumns()) {
                if (column.getType().isStructType()) {
                    addStructFieldNameToDict((StructType) column.getType());
                }
                addIntoDict(column.getName().toLowerCase(Locale.ROOT));
            }
        }

        for (Pair<String, View> pair : dumpInfo.getViewMap().values()) {
            // desensitized dbName
            addIntoDict(pair.first);
            // desensitized viewName
            View view = pair.second;
            addIntoDict(view.getName());
        }

        for (Map<String, Long> partitions : dumpInfo.getPartitionRowCountMap().values()) {
            // desensitized partitionName
            partitions.keySet().forEach(this::addIntoDict);
        }
        collectStmtDictInfo();
    }

    private void fillBuiltInWords() {
        desensitizedDict.put("*", "*");

        // table function default col name
        desensitizedDict.put("unnest", "unnest");
        desensitizedDict.put("key", "key");
        desensitizedDict.put("value", "value");
        desensitizedDict.put("generate_series", "generate_series");
        desensitizedDict.put("id", "id");
        desensitizedDict.put("segments", "segments");
        desensitizedDict.put("rows", "rows");
        desensitizedDict.put("size", "size");
        desensitizedDict.put("overlapped", "overlapped");
        desensitizedDict.put("delete_predicate", "delete_predicate");
    }

    private void addStructFieldNameToDict(StructType type) {
        for (StructField field : type.getFields()) {
            addIntoDict(field.getName());
            if (field.getType().isStructType()) {
                addStructFieldNameToDict((StructType) field.getType());
            }
        }
    }

    private void collectStmtDictInfo() {
        StatementBase statementBase = dumpInfo.getStatement();
        statementBase.accept(new Visitor(), null);
    }

    private void addIntoDict(String key) {
        if (desensitizedDict.get(key) == null) {
            desensitizedDict.put(key, idGenerator.getNextId().toString());
        }
    }

    private class Visitor extends AstTraverser<Void, Void> {

        @Override
        public Void visitRelation(Relation relation, Void context) {
            collectRelationDict(relation);
            return null;
        }

        @Override
        public Void visitSelect(SelectRelation relation, Void context) {
            collectRelationDict(relation);
            super.visitSelect(relation, context);
            return null;
        }

        @Override
        public Void visitJoin(JoinRelation relation, Void context) {
            collectRelationDict(relation);
            super.visitJoin(relation, context);
            return null;
        }

        @Override
        public Void visitSubqueryRelation(SubqueryRelation relation, Void context) {
            collectRelationDict(relation);
            super.visitSubqueryRelation(relation, context);
            return null;
        }

        @Override
        public Void visitSetOp(SetOperationRelation relation, Void context) {
            collectRelationDict(relation);
            super.visitSetOp(relation, context);
            return null;
        }

        @Override
        public Void visitCTE(CTERelation relation, Void context) {
            collectRelationDict(relation);
            super.visitCTE(relation, context);
            return null;
        }

        @Override
        public Void visitView(ViewRelation relation, Void context) {
            collectRelationDict(relation);
            super.visitView(relation, context);
            return null;
        }

        @Override
        public Void visitSubfieldExpr(SubfieldExpr subfieldExpr, Void context) {
            subfieldExpr.getFieldNames().stream().forEach(e -> addIntoDict(e.toLowerCase(Locale.ROOT)));
            visit(subfieldExpr.getChild(0));
            return null;
        }

        @Override
        public Void visitSlot(SlotRef slotRef, Void context) {
            String[] parts = slotRef.getColumnName().split("\\.");
            Arrays.stream(parts).forEach(e -> addIntoDict(e.toLowerCase(Locale.ROOT)));
            return null;
        }

        private void collectRelationDict(Relation relation) {
            if (CollectionUtils.isNotEmpty(relation.getColumnOutputNames())) {
                relation.getColumnOutputNames().stream().forEach(e -> addIntoDict(e.toLowerCase(Locale.ROOT)));
            }
            TableName tableName = relation.getResolveTableName();
            if (tableName == null) {
                return;
            }
            if (StringUtils.isNotEmpty(tableName.getCatalog())) {
                addIntoDict(tableName.getCatalog());
            }

            if (StringUtils.isNotEmpty(tableName.getDb())) {
                addIntoDict(tableName.getDb());
            }
            if (StringUtils.isNotEmpty(tableName.getTbl())) {
                addIntoDict(tableName.getTbl());
            }

            if (relation.getAlias() == null) {
                return;
            }
            tableName = relation.getAlias();
            if (StringUtils.isNotEmpty(tableName.getCatalog())) {
                addIntoDict(tableName.getCatalog());
            }

            if (StringUtils.isNotEmpty(tableName.getDb())) {
                addIntoDict(tableName.getDb());
            }
            if (StringUtils.isNotEmpty(tableName.getTbl())) {
                addIntoDict(tableName.getTbl());
            }
        }
    }

}
