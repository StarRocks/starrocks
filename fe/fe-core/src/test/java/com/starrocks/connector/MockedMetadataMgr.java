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


package com.starrocks.connector;

<<<<<<< HEAD
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
=======
import com.google.common.base.Strings;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.TemporaryTableMgr;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class MockedMetadataMgr extends MetadataMgr {
    private final Map<String, ConnectorMetadata> metadatas = new HashMap<>();
    private final LocalMetastore localMetastore;

    public MockedMetadataMgr(LocalMetastore localMetastore, ConnectorMgr connectorMgr) {
<<<<<<< HEAD
        super(localMetastore, connectorMgr, new ConnectorTblMetaInfoMgr());
=======
        super(localMetastore, new TemporaryTableMgr(), connectorMgr, new ConnectorTblMetaInfoMgr());
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        this.localMetastore = localMetastore;
    }

    public void registerMockedMetadata(String catalogName, ConnectorMetadata metadata) {
        metadatas.put(catalogName, metadata);
    }

    @Override
    public Optional<ConnectorMetadata> getOptionalMetadata(String catalogName) {
<<<<<<< HEAD
        if (catalogName == null || CatalogMgr.isInternalCatalog(catalogName)) {
=======
        if (Strings.isNullOrEmpty(catalogName) || CatalogMgr.isInternalCatalog(catalogName)) {
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            return Optional.of(localMetastore);
        } else {
            return Optional.ofNullable(metadatas.get(catalogName));
        }
    }
}
