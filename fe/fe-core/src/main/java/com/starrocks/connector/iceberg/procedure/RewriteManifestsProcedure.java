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

import com.starrocks.connector.exception.StarRocksConnectorException;
import com.starrocks.connector.iceberg.IcebergTableOperation;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import org.apache.iceberg.RewriteManifests;

import java.util.Collections;
import java.util.Map;

public class RewriteManifestsProcedure extends IcebergTableProcedure {
    private static final String PROCEDURE_NAME = "rewrite_manifests";

    private static final RewriteManifestsProcedure INSTANCE = new RewriteManifestsProcedure();

    public static RewriteManifestsProcedure getInstance() {
        return INSTANCE;
    }

    private RewriteManifestsProcedure() {
        super(
                PROCEDURE_NAME,
                Collections.emptyList(),
                IcebergTableOperation.REWRITE_MANIFESTS
        );
    }

    @Override
    public void execute(IcebergTableProcedureContext context, Map<String, ConstantOperator> args) {
        if (!args.isEmpty()) {
            throw new StarRocksConnectorException(
                    "invalid args. rewrite_manifests operation does not support any arguments");
        }

        RewriteManifests rewriteManifests = context.transaction().rewriteManifests();
        rewriteManifests.commit();
    }
}

