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

package com.starrocks.authorization;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Frame-free checks of the CONTEXTBASE privilege object: the {@code ON ALL CONTEXTBASES} wildcard
 * resolves to the reserved sentinel id, and its match semantics cover any specific base (the core
 * of a wildcard grant). Resolving a named base needs the catalog, so that path is left to the
 * integration/DDL suite.
 */
public class ContextBasePEntryObjectTest {

    private static ContextBasePEntryObject all() throws Exception {
        return (ContextBasePEntryObject) ContextBasePEntryObject.generate(Collections.singletonList("*"));
    }

    @Test
    public void testGenerateWildcard() throws Exception {
        ContextBasePEntryObject obj = all();
        assertEquals(PrivilegeBuiltinConstants.ALL_CONTEXTBASES_ID, obj.getId());
        assertTrue(obj.isFuzzyMatching(), "ON ALL CONTEXTBASES is a fuzzy-matching grant");
    }

    @Test
    public void testGenerateRejectsBadTokenCount() {
        assertThrows(PrivilegeException.class, () -> ContextBasePEntryObject.generate(Collections.emptyList()));
        assertThrows(PrivilegeException.class, () -> ContextBasePEntryObject.generate(Arrays.asList("a", "b")));
    }

    @Test
    public void testWildcardCoversSpecificBase() throws Exception {
        ContextBasePEntryObject specific = new ContextBasePEntryObject(5L);
        // A specific requested object is covered when the granted policy is the ALL wildcard.
        assertTrue(specific.match(all()), "a specific base must match an ON ALL CONTEXTBASES grant");
        // Same-id matches; different id does not.
        assertTrue(specific.match(new ContextBasePEntryObject(5L)));
        assertFalse(specific.match(new ContextBasePEntryObject(6L)));
        assertFalse(specific.isFuzzyMatching());
    }
}
