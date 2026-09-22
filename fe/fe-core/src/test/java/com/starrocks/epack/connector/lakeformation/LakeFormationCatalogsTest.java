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
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.server.CatalogMgr;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Whether a catalog is governed is read from what that catalog was created with, never from the resolved
 * cluster wide access control setting: `Config.access_control = lakeformation` must not turn Lake Formation
 * on for a catalog that never asked for it, because such a catalog has no role, no session tag and no way
 * to authorize anything.
 */
public class LakeFormationCatalogsTest {

    private static void catalogIs(Catalog catalog) {
        new MockUp<CatalogMgr>() {
            @Mock
            public Catalog getCatalogByName(String name) {
                return catalog;
            }
        };
    }

    private static Catalog catalogWith(Map<String, String> config) {
        return new Catalog(1L, "lf", config, "");
    }

    @Test
    public void testNoNameIsNotAGovernedCatalog() {
        assertFalse(LakeFormationCatalogs.isLakeFormationCatalog(null));
    }

    /** The internal catalog is answered without asking the catalog manager at all. */
    @Test
    public void testTheInternalCatalogIsNeverGoverned() {
        assertFalse(LakeFormationCatalogs.isLakeFormationCatalog(InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME));
    }

    /** A name that resolves to nothing is not governed - and must not throw on the way to saying so. */
    @Test
    public void testAnUnknownCatalogIsNotGoverned() {
        catalogIs(null);
        assertFalse(LakeFormationCatalogs.isLakeFormationCatalog("gone"));
    }

    @Test
    public void testACatalogWithoutStoredPropertiesIsNotGoverned() {
        catalogIs(catalogWith(null));
        assertFalse(LakeFormationCatalogs.isLakeFormationCatalog("lf"));
    }

    @Test
    public void testACatalogThatDidNotAskIsNotGoverned() {
        catalogIs(catalogWith(Map.of("type", "hive")));
        assertFalse(LakeFormationCatalogs.isLakeFormationCatalog("lf"));
    }

    @Test
    public void testACatalogThatAskedIsGoverned() {
        catalogIs(catalogWith(Map.of("type", "hive", "catalog.access.control", "lakeformation")));
        assertTrue(LakeFormationCatalogs.isLakeFormationCatalog("lf"));
    }
}
