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

package com.starrocks.type;

import com.google.gson.JsonParseException;
import com.google.gson.TypeAdapter;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.starrocks.persist.gson.GsonPostProcessable;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Objects;

/**
 * A native GEOGRAPHY or GEOMETRY type with its semantic descriptor.
 * <p>
 * Only {@code crs}, {@code edgeAlgorithm} and {@code srid} are persisted; the logical type and the coordinate
 * system follow from the primitive type. A GEO {@link ScalarType} that is not a {@code GeoType} is legacy metadata
 * written before descriptors were persisted: it has no descriptor and stays a plain {@code ScalarType}.
 */
public class GeoType extends ScalarType implements GsonPostProcessable {
    @SerializedName(value = "crs")
    @JsonAdapter(StrictStringAdapter.class)
    private String crs;

    @SerializedName(value = "edgeAlgorithm")
    @JsonAdapter(StrictStringAdapter.class)
    private String edgeAlgorithm;

    @SerializedName(value = "srid")
    @JsonAdapter(StrictIntegerAdapter.class)
    private Integer srid;

    // Derived from the primitive type and the persisted fields above.
    private GeoTypeDescriptor descriptor;

    // For GSON only; gsonPostProcess() validates what was read.
    private GeoType() {
        super();
    }

    GeoType(PrimitiveType primitive, GeoTypeDescriptor descriptor) {
        super(primitive);
        Objects.requireNonNull(descriptor).validate(primitive);
        this.descriptor = descriptor;
        this.crs = descriptor.crs();
        this.edgeAlgorithm = descriptor.edgeAlgorithm().name();
        this.srid = descriptor.srid();
    }

    @Override
    public GeoTypeDescriptor getGeoDescriptor() {
        return descriptor;
    }

    @Override
    public void gsonPostProcess() {
        if (crs == null || edgeAlgorithm == null) {
            throw new JsonParseException("Invalid geo type metadata: crs and edgeAlgorithm are required");
        }
        try {
            GeoTypeDescriptor loaded = GeoTypeDescriptor.of(getPrimitiveType(),
                    GeoTypeDescriptor.EdgeAlgorithm.valueOf(edgeAlgorithm), crs, srid);
            loaded.validate(getPrimitiveType());
            descriptor = loaded;
        } catch (IllegalArgumentException e) {
            throw new JsonParseException("Invalid geo type metadata: " + e.getMessage(), e);
        }
    }

    /** GSON would otherwise coerce numbers to strings. */
    static final class StrictStringAdapter extends TypeAdapter<String> {
        @Override
        public void write(JsonWriter out, String value) throws IOException {
            out.value(value);
        }

        @Override
        public String read(JsonReader in) throws IOException {
            if (in.peek() != JsonToken.STRING) {
                throw new JsonParseException("Invalid geo type metadata: expected a string at " + in.getPath());
            }
            return in.nextString();
        }
    }

    /** GSON would otherwise coerce numeric strings to integers. */
    static final class StrictIntegerAdapter extends TypeAdapter<Integer> {
        @Override
        public void write(JsonWriter out, Integer value) throws IOException {
            out.value(value);
        }

        @Override
        public Integer read(JsonReader in) throws IOException {
            if (in.peek() != JsonToken.NUMBER) {
                throw new JsonParseException("Invalid geo type metadata: expected an integer at " + in.getPath());
            }
            try {
                return new BigDecimal(in.nextString()).intValueExact();
            } catch (ArithmeticException | NumberFormatException e) {
                throw new JsonParseException("Invalid geo type metadata: srid must be an integer in the INT range", e);
            }
        }
    }
}
