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

import java.util.List;
import java.util.Optional;

import com.google.common.base.Preconditions;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.TableIndexes;
import com.starrocks.common.Pair;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexFactory {
    private static final Logger LOG = LoggerFactory.getLogger(IndexFactory.class);

    public static TableIndexes createIndexesFromCreateStmt(List<Index> indexes, OlapTable table) {
        if (CollectionUtils.isEmpty(indexes)) {
            return new TableIndexes();
        }

        // <indexValidMark, CheckResult>, indexId < 0 == not invalid
        Optional<Pair<Boolean, Boolean>>
                checkOptionResult = indexes.stream().map(index -> Pair.create(index.getIndexId() < 0, false))
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

    public static Index createIndexFromDef(Table table, IndexDef indexDef) {
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
}
