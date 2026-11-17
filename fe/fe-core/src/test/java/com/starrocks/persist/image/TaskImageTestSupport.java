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

package com.starrocks.persist.image;

import com.google.gson.JsonObject;
import com.google.gson.annotations.SerializedName;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.qe.VariableMgr;
import com.starrocks.server.GlobalStateMgr;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

/** Ensures the populated fixtures exercise every field in the existing v2 persistence contract. */
final class TaskImageTestSupport {
    private TaskImageTestSupport() {
    }

    static GlobalStateMgr global() {
        GlobalStateMgr global = LoadImageTestSupport.global();
        when(global.getVariableMgr()).thenReturn(new VariableMgr());
        return global;
    }

    static void assertAllPersistedFieldsPresent(Object value) {
        JsonObject json = GsonUtils.GSON.toJsonTree(value).getAsJsonObject();
        for (Field field : value.getClass().getDeclaredFields()) {
            SerializedName annotation = field.getAnnotation(SerializedName.class);
            if (annotation != null) {
                assertTrue(json.has(annotation.value()), "fixture does not exercise " + field.getName());
            }
        }
    }
}
