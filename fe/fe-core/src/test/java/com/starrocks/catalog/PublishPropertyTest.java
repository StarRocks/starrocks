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

import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.KeysType;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PublishPropertyTest {

    private static OlapTable table(boolean cloudNative, KeysType keysType) {
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.isCloudNativeTable()).thenReturn(cloudNative);
        Mockito.when(table.getKeysType()).thenReturn(keysType);
        return table;
    }

    private static OlapTable sharedDataPrimaryKeyTable() {
        return table(true, KeysType.PRIMARY_KEYS);
    }

    private static Map<String, String> written(String... keysAndValues) {
        Map<String, String> properties = new LinkedHashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            properties.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return properties;
    }

    @Test
    public void testValidateAndConsume() {
        // A recognized name is checked and taken out of the map, so the caller's leftover check sees
        // only what nobody recognized.
        Map<String, String> properties = written("pk_index_memtable_max_count", "8", "not_a_property", "1");
        PublishProperty.Changes changes =
                PublishProperty.validateAndExtract(properties, sharedDataPrimaryKeyTable());

        assertEquals(Map.of("pk_index_memtable_max_count", "8"), changes.upserts());
        assertTrue(changes.removals().isEmpty());
        assertEquals(written("not_a_property", "1"), properties);
    }

    @Test
    public void testUnsetValueRemoves() {
        // Every property in the first batch removes on the empty string, and each declares it for
        // itself, so both entry points read the same literal as a removal.
        for (String name : PublishProperty.names()) {
            assertEquals(java.util.Set.of(name), PublishProperty.parseChanges(written(name, "")).removals(), name);
        }

        Map<String, String> properties = written("pk_index_memtable_max_count", "");
        PublishProperty.Changes changes =
                PublishProperty.validateAndExtract(properties, sharedDataPrimaryKeyTable());

        assertTrue(changes.upserts().isEmpty());
        assertEquals(java.util.Set.of("pk_index_memtable_max_count"), changes.removals());
    }

    @Test
    public void testParseChangesNeitherChecksNorExtracts() {
        // An edit log entry was checked when the statement ran; checking it again would let a range
        // narrowed in a later version stop a replay. 65 is above what this property now accepts.
        Map<String, String> logged = written("pk_index_memtable_max_count", "65", "other", "1");
        PublishProperty.Changes changes = PublishProperty.parseChanges(logged);

        assertEquals(Map.of("pk_index_memtable_max_count", "65"), changes.upserts());
        assertEquals(written("pk_index_memtable_max_count", "65", "other", "1"), logged);
    }

    @Test
    public void testRejectedValues() {
        OlapTable table = sharedDataPrimaryKeyTable();

        assertThrows(SemanticException.class, () -> PublishProperty.validateAndExtract(
                written("pk_index_memtable_max_count", "eight"), table));
        // 64 is the highest this property accepts, and 1 the lowest.
        assertThrows(SemanticException.class, () -> PublishProperty.validateAndExtract(
                written("pk_index_memtable_max_count", "65"), table));
        assertThrows(SemanticException.class, () -> PublishProperty.validateAndExtract(
                written("pk_index_memtable_max_count", "0"), table));
        // Zero is inside the range only where a read site treats it as a switch.
        assertEquals(Map.of("pk_index_rebuild_files_threshold", "0"),
                PublishProperty.validateAndExtract(
                        written("pk_index_rebuild_files_threshold", "0"), table).upserts());
    }

    @Test
    public void testScopeIsCheckedAgainstTheTablesOwnStorage() {
        // A shared-data cluster can hold a table whose tablets are not cloud-native, and that table
        // must not accept a property its tablets never see.
        assertThrows(SemanticException.class, () -> PublishProperty.validateAndExtract(
                written("pk_index_memtable_max_count", "8"), table(false, KeysType.PRIMARY_KEYS)));
        assertThrows(SemanticException.class, () -> PublishProperty.validateAndExtract(
                written("pk_index_memtable_max_count", "8"), table(true, KeysType.DUP_KEYS)));
    }

    @Test
    public void testRoutingPredicates() {
        assertTrue(PublishProperty.declaresAny(written("pk_index_memtable_max_count", "8", "other", "1")));
        assertFalse(PublishProperty.declaresAny(written("other", "1")));

        assertTrue(PublishProperty.names().contains("pk_index_memtable_max_count"));
        assertFalse(PublishProperty.names().contains("other"));
    }

    @Test
    public void testFilterKeepsOnlyRegisteredNames() {
        Map<String, String> stored = written("replication_num", "1", "pk_index_memtable_max_count", "8");
        assertEquals(Map.of("pk_index_memtable_max_count", "8"), PublishProperty.selectFrom(stored));
        assertTrue(PublishProperty.selectFrom(written("replication_num", "1")).isEmpty());
    }
}
