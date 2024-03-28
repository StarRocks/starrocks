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

import com.starrocks.catalog.Index;
import com.starrocks.catalog.TableIndexes;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexFactory {

    private static final Logger LOG = LoggerFactory.getLogger(IndexFactory.class);

    // TODO: More check action for indexId in the future
    public static TableIndexes createIndexesFromCreateStmt(List<Index> indexes) {
        if (CollectionUtils.isEmpty(indexes)) {
            return new TableIndexes();
        }

        return new TableIndexes(indexes);
    }

    // TODO: More check action for indexId in the future
    public static Index createIndexFromDef(IndexDef indexDef) {
        return new Index(indexDef.getIndexName(), indexDef.getColumns(),
                indexDef.getIndexType(), indexDef.getComment(), indexDef.getProperties());
    }
}
