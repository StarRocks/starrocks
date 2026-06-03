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
import com.starrocks.type.Type;

import java.time.Instant;
import java.time.LocalDateTime;
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
        // StarRocks canonical date/datetime text ("yyyy-MM-dd[ HH:mm:ss[.ffffff]]").
        // This is the form the backend expects when it parses a value with
        // datum_from_string (e.g. external split-boundary tuples); an ISO-8601
        // instant ("...T...Z") makes the storage engine reject every
        // date/datetime boundary and silently skip the split.
        LocalDateTime dateTime = LocalDateTime.ofEpochSecond(seconds, (int) nanos, ZoneOffset.UTC);
        switch (type.getPrimitiveType()) {
            case DATE:
                return dateTime.format(DateUtils.DATE_FORMATTER);
            case DATETIME:
                if (nanos == 0) {
                    return dateTime.format(DateUtils.DATE_TIME_FORMATTER);
                }
                return dateTime.format(DateUtils.DATE_TIME_FORMATTER) + "." + String.format("%06d", nanos / 1000);
            default:
                return Instant.ofEpochSecond(seconds, nanos).toString();
        }
    }

    @Override
    protected int compareToImpl(Variant other) {
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
