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

package com.starrocks.utframe;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import com.google.gson.JsonSyntaxException;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.stream.JsonReader;
import com.starrocks.persist.gson.GsonUtils;
import com.starrocks.persist.gson.RuntimeTypeAdapterFactory;
import mockit.Delegate;
import mockit.Expectations;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.List;

public class GsonReflectUtils {
    private static final Logger LOG = LogManager.getLogger(GsonReflectUtils.class);

    // Remove the `RuntimeTypeAdapterFactory<baseType>` from the GsonBuilder `builder`
    public static GsonBuilder removeRuntimeTypeAdapterFactoryForBaseType(GsonBuilder builder, Class<?> baseType) {
        List<String> fieldNames = List.of("factories", "hierarchyFactories");
        for (String fieldName : fieldNames) {
            List<TypeAdapterFactory> factories;
            try {
                Field field = GsonBuilder.class.getDeclaredField(fieldName);
                factories = (List<TypeAdapterFactory>) FieldUtils.readField(field, builder, true);
            } catch (Exception ex) {
                throw new RuntimeException(String.format("Failed to hijack GSON.%s by reflection.", fieldName), ex);
            }
            for (TypeAdapterFactory factory : factories) {
                // remove the repeated registered factory
                if (!(factory instanceof RuntimeTypeAdapterFactory)) {
                    continue;
                }
                try {
                    Field baseTypeField = RuntimeTypeAdapterFactory.class.getDeclaredField("baseType");
                    Class<?> type = (Class<?>) FieldUtils.readField(baseTypeField, factory, true);
                    if (baseType == type) {
                        LOG.warn("Find baseType: {} from factory:{}, removing it!", baseType, fieldName);
                        factories.remove(factory);
                        break;
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(
                            String.format("Failed to hijack RuntimeTypeAdapterFactory.%s by reflection.", baseType),
                            ex);
                }
            }
        }
        return builder;
    }

    public static Expectations partialMockGsonExpectations(Gson gson) {
        return new Expectations(GsonUtils.GSON) {
            {
                GsonUtils.GSON.toJson(any);
                result = new Delegate() {
                    public String toJson(Object src) {
                        return gson.toJson(src);
                    }
                };
                minTimes = 0;

                GsonUtils.GSON.fromJson((JsonReader) any, (Type) any);
                result = new Delegate() {
                    public <T> T fromJson(JsonReader reader, Type typeOfT) throws JsonIOException, JsonSyntaxException {
                        return gson.fromJson(reader, typeOfT);
                    }
                };
            }
        };
    }
}
