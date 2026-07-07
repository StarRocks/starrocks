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

package com.starrocks.context.policy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CollectionTypePolicyTest {

    @Test
    public void testKnowledgeAllowsAllEntityTypes() {
        CollectionTypePolicy.check("knowledge", "object");
        CollectionTypePolicy.check("knowledge", "doc");
        CollectionTypePolicy.check("knowledge", "page");
        CollectionTypePolicy.check("knowledge", "homepage");
        CollectionTypePolicy.check("knowledge", "derived_page");
        CollectionTypePolicy.check("knowledge", "derived_homepage");
    }

    @Test
    public void testChannelRejectsEverythingButPage() {
        CollectionTypePolicy.check("channel", "page");
        for (String bad : new String[] {"object", "doc", "homepage", "derived_page", "derived_homepage"}) {
            IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> CollectionTypePolicy.check("channel", bad));
            Assertions.assertTrue(ex.getMessage().contains("not allowed"));
        }
    }

    @Test
    public void testMemoryRejectsObjectAndDoc() {
        CollectionTypePolicy.check("memory", "page");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CollectionTypePolicy.check("memory", "object"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CollectionTypePolicy.check("memory", "doc"));
    }

    @Test
    public void testUnknownCollectionTypeRejected() {
        IllegalArgumentException ex = Assertions.assertThrows(IllegalArgumentException.class,
                () -> CollectionTypePolicy.check("imaginary", "page"));
        Assertions.assertTrue(ex.getMessage().contains("unknown collection_type"));
    }

    @Test
    public void testCaseInsensitive() {
        CollectionTypePolicy.check("KNOWLEDGE", "PAGE");
        CollectionTypePolicy.check("Channel", "Page");
    }

    @Test
    public void testMissingInputsRejected() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CollectionTypePolicy.check(null, "page"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> CollectionTypePolicy.check("knowledge", null));
    }

    @Test
    public void testIsValidCollectionType() {
        Assertions.assertTrue(CollectionTypePolicy.isValidCollectionType("knowledge"));
        Assertions.assertTrue(CollectionTypePolicy.isValidCollectionType("skill"));
        Assertions.assertTrue(CollectionTypePolicy.isValidCollectionType("memory"));
        Assertions.assertTrue(CollectionTypePolicy.isValidCollectionType("task_summary"));
        Assertions.assertTrue(CollectionTypePolicy.isValidCollectionType("channel"));
        Assertions.assertFalse(CollectionTypePolicy.isValidCollectionType("imaginary"));
        Assertions.assertFalse(CollectionTypePolicy.isValidCollectionType(null));
    }
}
