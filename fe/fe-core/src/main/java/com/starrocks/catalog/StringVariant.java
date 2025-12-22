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

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.util.StringUtils;
import com.starrocks.thrift.TVariant;
import com.starrocks.type.Type;
import com.starrocks.type.TypeSerializer;

/*
 * StringVariant is for type CHAR, VARCHAR, BINARY, VARBINARY and HLL
 */
public class StringVariant extends Variant {

    @SerializedName(value = "value")
    protected final String value;

    public StringVariant(Type type, String value) {
        super(type);
        this.value = value;
    }

    @Override
    public long getLongValue() {
        return IntVariant.parseLong(value);
    }

    @Override
    public String getStringValue() {
        return value;
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
        // compare string with utf-8 byte array, same with DM,BE,StorageEngine
        return StringUtils.compareStringWithUTF8ByteArray(value, other.getStringValue());
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || !(object instanceof StringVariant)) {
            return false;
        }
        StringVariant other = (StringVariant) object;
        return this.value.equals(other.value);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }
}
