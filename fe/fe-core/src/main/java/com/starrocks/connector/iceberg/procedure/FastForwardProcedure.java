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

package com.starrocks.connector.iceberg.procedure;

import com.starrocks.catalog.Type;
import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;

import java.util.Arrays;
import java.util.Map;

public class FastForwardProcedure extends IcebergTableProcedure {
    private static final String PROCEDURE_NAME = "fast_forward";

    private static final String FROM_BRANCH = "from_branch";
    private static final String TO_BRANCH = "to_branch";

    private static final FastForwardProcedure INSTANCE = new FastForwardProcedure();

    public static FastForwardProcedure getInstance() {
        return INSTANCE;
    }

    private FastForwardProcedure() {
        super(
                PROCEDURE_NAME,
                Arrays.asList(
                        new NamedArgument(FROM_BRANCH, Type.VARCHAR, true),
                        new NamedArgument(TO_BRANCH, Type.VARCHAR, true)
                ),
                IcebergTableOperation.FAST_FORWARD
        );
    }

    @Override
    public void execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (args.size() != 2) {
            throw new StarRocksConnectorException("invalid args. fast forward must contain `from branch` and `to branch`");
        }

        if (!args.containsKey(FROM_BRANCH) || !args.containsKey(TO_BRANCH)) {
            throw new StarRocksConnectorException("missing required argument: %s or %s", FROM_BRANCH, TO_BRANCH);
        }

        String from = args.get(FROM_BRANCH)
                .castTo(Type.VARCHAR)
                .map(ConstantOperator::getChar)
                .orElseThrow(() -> new StarRocksConnectorException("invalid argument type for %s, expected STRING",
                        FROM_BRANCH));

        String to = args.get(TO_BRANCH)
                .castTo(Type.VARCHAR)
                .map(ConstantOperator::getChar)
                .orElseThrow(() -> new StarRocksConnectorException("invalid argument type for %s, expected STRING",
                        TO_BRANCH));

        context.transaction().manageSnapshots().fastForwardBranch(from, to).commit();
    }
}