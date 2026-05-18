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

package com.starrocks.lake.bookmark;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.starrocks.catalog.MvId;
import com.starrocks.lake.bookmark.ReferenceHolder.Custom;
import com.starrocks.lake.bookmark.ReferenceHolder.Mv;
import com.starrocks.lake.bookmark.ReferenceHolder.Type;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class ReferenceHolderTest {

    @Test
    public void testHolder() {
        Mv mv1 = new Mv(new MvId(1L, 100L));
        Mv mv2 = new Mv(new MvId(1L, 100L));
        assertEquals(mv1, mv2);
        assertEquals(mv1.hashCode(), mv2.hashCode());

        Custom c1 = new Custom("alpha");
        Custom c2 = new Custom("alpha");
        assertEquals(c1, c2);
        assertEquals(c1.hashCode(), c2.hashCode());

        Mv mvOther = new Mv(new MvId(1L, 200L));
        assertNotEquals(mv1, mvOther);
        Custom cOther = new Custom("beta");
        assertNotEquals(c1, cOther);

        // Cross-type — never equal even when name() outputs match.
        assertNotEquals(mv1, c1);

        assertEquals("MATERIALIZED_VIEW:1-100", mv1.toString());
        assertEquals("CUSTOM:alpha", c1.toString());

        assertEquals(Type.MATERIALIZED_VIEW, mv1.getType());
        assertEquals(Type.CUSTOM, c1.getType());

        Gson gson = new GsonBuilder()
                .registerTypeAdapterFactory(ReferenceHolder.typeAdapterFactory())
                .create();

        String mvJson = gson.toJson(mv1, ReferenceHolder.class);
        ReferenceHolder mvBack = gson.fromJson(mvJson, ReferenceHolder.class);
        assertInstanceOf(Mv.class, mvBack);
        assertEquals(mv1, mvBack);

        String customJson = gson.toJson(c1, ReferenceHolder.class);
        ReferenceHolder customBack = gson.fromJson(customJson, ReferenceHolder.class);
        assertInstanceOf(Custom.class, customBack);
        assertEquals(c1, customBack);
    }
}
