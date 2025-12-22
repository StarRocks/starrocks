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

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.util.DateUtils;
import com.starrocks.thrift.TVariant;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Objects;

/*
 * DateVariant is for type DATE, DATETIME and TIME
 */
public class DateVariant extends Variant {

    @SerializedName(value = "seconds")
    protected final long seconds;

    @SerializedName(value = "nanos")
    protected final long nanos;

    public DateVariant(Type type, String value) {
        this(type, DateUtils.parseStrictDateTime(value).toInstant(ZoneOffset.UTC));
    }

    public DateVariant(Type type, Instant value) {
        super(type);
        this.seconds = value.getEpochSecond();
        this.nanos = value.getNano();
    }

    @Override
    public long getLongValue() {
        // Micro seconds
        return seconds * 1000000 + (nanos / 1000);
    }

    @Override
    public String getStringValue() {
        return Instant.ofEpochSecond(seconds, nanos).toString();
    }

    @Override
    public TVariant toThrift() {
        TVariant variant = new TVariant();
        variant.setType(TypeSerializer.toThrift(type));
        variant.setValue(getStringValue());
        return variant;
    }

    @Override
    public int compareTo(Variant other) {
        Preconditions.checkArgument(other instanceof DateVariant, other);
        DateVariant otherDateTime = (DateVariant) other;

        int result = Long.compare(this.seconds, otherDateTime.seconds);
        if (result != 0) {
            return result;
        }
        return Long.compare(this.nanos, otherDateTime.nanos);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || !(object instanceof DateVariant)) {
            return false;
        }
        DateVariant other = (DateVariant) object;
        return this.seconds == other.seconds && this.nanos == other.nanos;
    }

    @Override
    public int hashCode() {
        return Objects.hash(seconds, nanos);
    }
}
