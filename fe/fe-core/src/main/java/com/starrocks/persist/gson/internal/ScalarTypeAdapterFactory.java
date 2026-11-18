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

package com.starrocks.persist.gson.internal;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.internal.Streams;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.starrocks.type.GeoTypeDescriptor;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.ScalarType;

import java.io.IOException;

public class ScalarTypeAdapterFactory implements TypeAdapterFactory {
    @Override
    public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> type) {
        if (type.getRawType() != ScalarType.class) {
            return null;
        }
        TypeAdapter<T> delegate = gson.getDelegateAdapter(this, type);
        return new TypeAdapter<T>() {
            @Override
            public void write(JsonWriter out, T value) throws IOException {
                GeoTypeDescriptor descriptor = ((ScalarType) value).getGeoDescriptor();
                if (descriptor == null) {
                    // Legacy catalog types remain unresolved, including when checkpointed again.
                    delegate.write(out, value);
                    return;
                }
                JsonObject json = delegate.toJsonTree(value).getAsJsonObject();
                JsonObject geo = new JsonObject();
                geo.addProperty("crs", descriptor.crs());
                geo.addProperty("edgeAlgorithm", descriptor.edgeAlgorithm().name());
                if (descriptor.srid() != null) {
                    geo.addProperty("srid", descriptor.srid());
                }
                json.add("geo", geo);
                Streams.write(json, out);
            }

            @Override
            @SuppressWarnings("unchecked")
            public T read(JsonReader in) throws IOException {
                JsonObject json = Streams.parse(in).getAsJsonObject();
                T value = delegate.fromJsonTree(json);
                if (!json.has("geo")) {
                    return value;
                }
                ScalarType scalar = (ScalarType) value;
                if (!scalar.isGeoType()) {
                    throw new JsonParseException("geo requires a GEOGRAPHY or GEOMETRY type");
                }
                if (!json.get("geo").isJsonObject()) {
                    throw new JsonParseException("geo must be an object");
                }
                JsonObject geo = json.getAsJsonObject("geo");
                String crs = stringField(geo, "crs");
                String edge = stringField(geo, "edgeAlgorithm");
                Integer srid = sridField(geo);
                boolean geography = scalar.getPrimitiveType() == PrimitiveType.GEOGRAPHY;
                try {
                    GeoTypeDescriptor descriptor = new GeoTypeDescriptor(
                            geography ? GeoTypeDescriptor.LogicalType.GEOGRAPHY : GeoTypeDescriptor.LogicalType.GEOMETRY,
                            geography ? GeoTypeDescriptor.CoordinateSystem.SPHERICAL
                                    : GeoTypeDescriptor.CoordinateSystem.CARTESIAN,
                            GeoTypeDescriptor.EdgeAlgorithm.valueOf(edge), crs, srid);
                    return (T) ScalarType.createGeoType(scalar.getPrimitiveType(), descriptor);
                } catch (IllegalArgumentException e) {
                    throw new JsonParseException("Invalid geo crs, edgeAlgorithm or srid: " + e.getMessage(), e);
                }
            }
        }.nullSafe();
    }

    private static String stringField(JsonObject geo, String name) {
        JsonElement value = geo.get(name);
        if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isString()) {
            throw new JsonParseException("geo." + name + " must be a string");
        }
        return value.getAsString();
    }

    private static Integer sridField(JsonObject geo) {
        JsonElement value = geo.get("srid");
        if (value == null || value.isJsonNull()) {
            return null;
        }
        if (!value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
            throw new JsonParseException("geo.srid must be an integer");
        }
        try {
            return value.getAsBigDecimal().intValueExact();
        } catch (ArithmeticException | NumberFormatException e) {
            throw new JsonParseException("geo.srid must be an integer in the INT range", e);
        }
    }
}
