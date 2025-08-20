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

import com.google.common.collect.Lists;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.RandomDistributionDesc;

import java.util.List;

/**
 * Builder class to centralize the creation of DistributionInfo from various DistributionDesc types.
 */
public class DistributionInfoBuilder {

    /**
     * Build DistributionInfo from a DistributionDesc
     */
    public static DistributionInfo build(DistributionDesc distributionDesc, List<Column> columns) throws DdlException {
        if (distributionDesc instanceof RandomDistributionDesc) {
            return buildRandomDistributionInfo((RandomDistributionDesc) distributionDesc, columns);
        } else if (distributionDesc instanceof HashDistributionDesc) {
            return buildHashDistributionInfo((HashDistributionDesc) distributionDesc, columns);
        } else {
            throw new DdlException("Unsupported distribution type: " + distributionDesc.getClass().getSimpleName());
        }
    }

    /**
     * Build RandomDistributionInfo from RandomDistributionDesc
     */
    public static RandomDistributionInfo buildRandomDistributionInfo(RandomDistributionDesc randomDistributionDesc,
                                                                     List<Column> columns) {
        return new RandomDistributionInfo(randomDistributionDesc.getBuckets());
    }

    /**
     * Build HashDistributionInfo from HashDistributionDesc
     */
    public static HashDistributionInfo buildHashDistributionInfo(HashDistributionDesc hashDistributionDesc,
                                                                 List<Column> columns) throws DdlException {
        List<Column> distributionColumns = Lists.newArrayList();

        // check and get a distribution column
        for (String colName : hashDistributionDesc.getDistributionColumnNames()) {
            boolean find = false;
            for (Column column : columns) {
                if (column.getName().equalsIgnoreCase(colName)) {
                    if (!column.isKey() && column.getAggregationType() != AggregateType.NONE) {
                        throw new DdlException("Distribution column[" + colName + "] is not key column");
                    }

                    if (!column.getType().canDistributedBy()) {
                        throw new DdlException(column.getType() + " column can not be distribution column");
                    }

                    distributionColumns.add(column);
                    find = true;
                    break;
                }
            }
            if (!find) {
                throw new DdlException("Distribution column[" + colName + "] does not found");
            }
        }

        return new HashDistributionInfo(hashDistributionDesc.getBuckets(), distributionColumns);
    }
}
