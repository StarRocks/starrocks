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

package com.starrocks.analysis;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.common.InvertedIndexParams;
import com.starrocks.common.InvertedIndexParams.InvertedIndexImpType;
import com.starrocks.common.Pair;
import com.starrocks.sql.analyzer.SemanticException;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.common.InvertedIndexParams.CommonIndexParamKey.IMP_LIB;
import static com.starrocks.common.InvertedIndexParams.InvertedIndexImpType.CLUCENE;

public class TableIndexManager {

    private static final Logger LOG = LoggerFactory.getLogger(TableIndexManager.class);

    private static class InstanceHolder {

        private static final TableIndexManager INSTANCE = new TableIndexManager();
    }

    public static TableIndexManager getInstance() {
        return InstanceHolder.INSTANCE;
    }

    private TableIndexManager() {
    }

    public static void checkInvertedIndexValid(Column column, Map<String, String> properties) {
        if (!(column.getType() instanceof ScalarType)) {
            throw new SemanticException("The inverted index can only be build on column with type of scalar type.");
        }

        String impLibKey = IMP_LIB.name().toLowerCase(Locale.ROOT);
        if (properties.containsKey(impLibKey)) {
            String impValue = properties.get(impLibKey);
            if (!CLUCENE.name().equalsIgnoreCase(impValue)) {
                throw new SemanticException("Only support clucene implement for now");
            }
        }

        InvertedIndexUtil.checkInvertedIndexParser(column.getName(), column.getPrimitiveType(), properties);
    }


    public TableIndexes createIndexesFromCreateStmt(List<Index> indexes, OlapTable table) {
        if (CollectionUtils.isEmpty(indexes)) {
            return new TableIndexes();
        }

        // <indexValidMark, CheckResult>, indexId < 0 == not invalid
        Optional<Pair<Boolean, Boolean>> checkOptionResult = indexes.stream().map(index -> Pair.create(index.getIndexId() < 0, false))
                .reduce((p1, p2) -> Pair.create(p2.first, (p1.first ^ p2.first) || p2.second || p1.second));
        Preconditions.checkArgument(checkOptionResult.isPresent());
        Pair<Boolean, Boolean> checkResultPair = checkOptionResult.get();

        // if all indexIds are invalid or not invalid, xor result will be false, or else true
        Preconditions.checkArgument(!checkResultPair.second, "All status of index indexId should be consistent");
        boolean invalidIndexMark = checkResultPair.first;

        if (invalidIndexMark) {
            if (table == null) {
                for (int idx = 0; idx < indexes.size(); idx++) {
                    indexes.get(idx).setIndexId(idx);
                }
            } else {
                for (Index index : indexes) {
                    index.setIndexId(table.incAndGetMaxIndexId());
                }
            }
        } else {
            if (table != null) {
                // table.getMaxIndexId() should >= max(index.getIndexId()) + 1
                Optional<Index> optionalTableIndex =
                        indexes.stream().filter(index -> index.getIndexId() >= table.getMaxIndexId()).findAny();
                optionalTableIndex.ifPresent(
                        index -> {
                            LOG.warn("Non-Consistence problem found, table index id {} <= maxIndexId {}", table.getMaxIndexId(),
                                    index.getIndexId());
                            table.setMaxIndexId(index.getIndexId() + 1);
                        });
            }
        }

        return new TableIndexes(indexes);
    }

    public Index createIndexFromDef(Table table, IndexDef indexDef) {
        Index index;
        if (table instanceof OlapTable) {
            index = new Index(((OlapTable) table).incAndGetMaxIndexId(), indexDef.getIndexName(), indexDef.getColumns(),
                    indexDef.getIndexType(), indexDef.getComment(), indexDef.getProperties());
        } else {
            index = new Index(indexDef.getIndexName(), indexDef.getColumns(),
                    indexDef.getIndexType(), indexDef.getComment(), indexDef.getProperties());
        }
        return index;
    }

    public List<Index> getIndexesByIds(OlapTable table, List<Long> indexIds) {
        return table.getIndexes().stream().filter(index -> indexIds.contains(index.getIndexId())).collect(Collectors.toList());
    }

    public List<Index> getIndexesByNames(OlapTable table, List<String> indexNames) {
        return table.getIndexes().stream().filter(index -> indexNames.contains(index.getIndexName()))
                .collect(Collectors.toList());
    }
}
