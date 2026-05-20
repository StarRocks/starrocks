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

package com.starrocks.lake.changes;

import com.starrocks.catalog.Column;
import com.starrocks.thrift.TChangesMetaKind;
import com.starrocks.type.IntegerType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests {@link ChangesMetaDescriptor#resolve}: the per-relation name resolver
 * that picks the kind's default name when nothing collides and otherwise mints
 * a deterministic alternate.
 */
public class ChangesMetaDescriptorTest {

    @Test
    public void testResolveUsesDefaultsWhenNoConflict() {
        List<ChangesMetaDescriptor> descriptors = ChangesMetaDescriptor.resolve(
                schemaOf("k", "v"));
        assertEquals(2, descriptors.size());
        assertEquals(TChangesMetaKind.CHANGE_TYPE, descriptors.get(0).kind());
        assertEquals("__CHANGE_TYPE__", descriptors.get(0).name());
        assertEquals(IntegerType.TINYINT, descriptors.get(0).type());
        assertEquals(TChangesMetaKind.ROW_VERSION, descriptors.get(1).kind());
        assertEquals("__ROW_VERSION__", descriptors.get(1).name());
        assertEquals(IntegerType.BIGINT, descriptors.get(1).type());
    }

    @Test
    public void testResolveFallsBackOnConflict() {
        // The conflict checks are case-insensitive — mixed-case shadow on
        // CHANGE_TYPE forces the alternate name.
        List<ChangesMetaDescriptor> descriptors = ChangesMetaDescriptor.resolve(
                schemaOf("k", "__Change_Type__"));
        assertEquals("__CHANGE_TYPE_1__", descriptors.get(0).name());
        assertEquals("__ROW_VERSION__", descriptors.get(1).name());
    }

    @Test
    public void testResolveChainsCandidatesPastExistingSuffix() {
        // Both __CHANGE_TYPE__ and __CHANGE_TYPE_1__ are taken; the resolver
        // must keep walking until it finds an unused slot.
        List<ChangesMetaDescriptor> descriptors = ChangesMetaDescriptor.resolve(
                schemaOf("__CHANGE_TYPE__", "__CHANGE_TYPE_1__"));
        assertEquals("__CHANGE_TYPE_2__", descriptors.get(0).name());
        assertEquals("__ROW_VERSION__", descriptors.get(1).name());
    }

    @Test
    public void testResolveDoesNotReuseAcrossKinds() {
        // When both defaults collide, each kind must pick a distinct alternate;
        // the second resolution sees the first's choice as occupied.
        List<ChangesMetaDescriptor> descriptors = ChangesMetaDescriptor.resolve(
                schemaOf("__CHANGE_TYPE__", "__ROW_VERSION__"));
        assertEquals("__CHANGE_TYPE_1__", descriptors.get(0).name());
        assertEquals("__ROW_VERSION_1__", descriptors.get(1).name());
    }

    private static List<Column> schemaOf(String... names) {
        List<Column> cols = new ArrayList<>(names.length);
        for (String n : names) {
            cols.add(new Column(n, IntegerType.INT));
        }
        return cols;
    }
}
