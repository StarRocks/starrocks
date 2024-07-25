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

package com.starrocks.catalog.system.function;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class CboStatsShowExclusion implements GenericFunction {
    private static final Logger LOG = LogManager.getLogger(CboStatsShowExclusion.class);

    public static final String FN_NAME = FunctionSet.CBO_STATS_SHOW_EXCLUSION;

    @Override
    public void init(FunctionCallExpr node, ConnectContext context) {
        genericSystemFunctionCheck(node);
        if (node.getChildren().size() != 0) {
            throw new SemanticException(FN_NAME + " input parameter must be empty", node.getPos());
        }
    }

    @Override
    public void prepare(FunctionCallExpr functionCallExpr, ConnectContext context) {
        //do nothing,refer to the behavior of 'SHOW ANALYZE JOB [WHERE predicate]'
    }

    @Override
    public ConstantOperator evaluate(List<ConstantOperator> arguments) {
        //mock data
        List<String> mock = new ArrayList<>();
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setPrettyPrinting();
        Gson gson = gsonBuilder.create();
        String showInfo = gson.toJson(mock);

        return ConstantOperator.createVarchar(showInfo);
    }
}

