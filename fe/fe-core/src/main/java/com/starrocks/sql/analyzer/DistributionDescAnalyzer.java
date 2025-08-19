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

import com.starrocks.catalog.DistributionInfo.DistributionInfoType;
import com.starrocks.sql.ast.DistributionDesc;
import com.starrocks.sql.ast.HashDistributionDesc;
import com.starrocks.sql.ast.RandomDistributionDesc;

import java.util.Set;

public class DistributionDescAnalyzer {
    
    public static void analyze(DistributionDesc distributionDesc, Set<String> colSet) {
        if (distributionDesc.getType() == DistributionInfoType.HASH) {
            analyzeHashDistribution((HashDistributionDesc) distributionDesc, colSet);
        } else if (distributionDesc.getType() == DistributionInfoType.RANDOM) {
            analyzeRandomDistribution((RandomDistributionDesc) distributionDesc, colSet);
        }
    }

    private static void analyzeHashDistribution(HashDistributionDesc distributionDesc, Set<String> colSet) {
        if (distributionDesc.getBuckets() < 0) {
            throw new SemanticException("Number of hash distribution is zero.");
        }

        if (distributionDesc.getDistributionColumnNames() == null || distributionDesc.getDistributionColumnNames().isEmpty()) {
            throw new SemanticException("Number of hash column is zero.");
        }
        for (String columnName : distributionDesc.getDistributionColumnNames()) {
            if (!colSet.contains(columnName)) {
                throw new SemanticException("Distribution column(" + columnName + ") doesn't exist.");
            }
        }
    }

    private static void analyzeRandomDistribution(RandomDistributionDesc distributionDesc, Set<String> colSet) {
        if (distributionDesc.getBuckets() < 0) {
            throw new SemanticException("Number of random distribution is zero.");
        }
    }
}