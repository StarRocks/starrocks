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

package com.starrocks.epack.connector.lakeformation;

import com.starrocks.catalog.Catalog;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;

/**
 * The single answer to "is this catalog governed by Lake Formation".
 *
 * Everything that has to branch on it - the statement guard, the SHOW CREATE projection - asks here, so that
 * they cannot drift apart or disagree with what the connector itself decided at creation time.
 */
public final class LakeFormationCatalogs {

    private LakeFormationCatalogs() {
    }

    /**
     * Reads the catalog's own stored properties, not the resolved access control value: the cluster wide
     * Config.access_control must never turn Lake Formation on for a catalog that did not ask for it.
     */
    public static boolean isLakeFormationCatalog(String catalogName) {
        if (catalogName == null || CatalogMgr.isInternalCatalog(catalogName)) {
            return false;
        }
        Catalog catalog = GlobalStateMgr.getCurrentState().getCatalogMgr().getCatalogByName(catalogName);
        if (catalog == null || catalog.getConfig() == null) {
            return false;
        }
        return LakeFormationCatalogProperties.isRequested(catalog.getConfig());
    }
}
