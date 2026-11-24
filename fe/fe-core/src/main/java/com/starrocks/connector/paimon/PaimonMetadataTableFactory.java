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

package com.starrocks.connector.paimon;

import com.starrocks.catalog.PaimonTable;
import com.starrocks.catalog.Table;
import com.starrocks.connector.metadata.AbstractMetadataTableFactory;
import com.starrocks.connector.metadata.MetadataTableType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;

public final class PaimonMetadataTableFactory implements AbstractMetadataTableFactory {
    public static final PaimonMetadataTableFactory INSTANCE = new PaimonMetadataTableFactory();

    @Override
    public Table createTable(ConnectContext context, String catalog, String db, String name, MetadataTableType type) {
        if (type != MetadataTableType.PAIMON_GLOBAL_INDEX || context == null || !context.isMetadataContext()) {
            throw new IllegalArgumentException("Unsupported Paimon metadata relation");
        }
        Table origin = GlobalStateMgr.getCurrentState().getMetadataMgr().getTable(context, catalog, db, name);
        if (!(origin instanceof PaimonTable)) {
            throw new IllegalArgumentException("Paimon Global Index requires a Paimon data table");
        }
        return new PaimonGlobalIndexMetadataTable((PaimonTable) origin);
    }
}
