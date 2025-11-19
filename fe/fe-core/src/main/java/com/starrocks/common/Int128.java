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

package com.starrocks.common;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;

import java.math.BigInteger;
import java.util.Objects;

public class Int128 implements Comparable<Int128> {

    @SerializedName(value = "high")
    protected final long high;

    @SerializedName(value = "low")
    protected final long low;

    public static Int128 of(BigInteger value) {
        Preconditions.checkArgument(value.bitLength() < 128,
                "Value out of signed 128-bit range: " + value);

        long high = value.shiftRight(64).longValue();
        long low = value.longValue();
        return new Int128(high, low);
    }

    public static Int128 of(String value) {
        return Int128.of(new BigInteger(value));
    }

    public static Int128 of(long value) {
        return new Int128(value);
    }

    private Int128(long high, long low) {
        this.high = high;
        this.low = low;
    }

    private Int128(long value) {
        this(value >> 63, value);
    }

    public long getHigh() {
        return high;
    }

    public long getLow() {
        return low;
    }

    public boolean isInLong() {
        return high == (low >> 63);
    }

    // For numbers outside long range, returns the low 64 bits
    public long toLong() {
        return low;
    }

    public byte[] toByteArray() {
        return new byte[] {
                (byte) ((high >> 56) & 0xFFL),
                (byte) ((high >> 48) & 0xFFL),
                (byte) ((high >> 40) & 0xFFL),
                (byte) ((high >> 32) & 0xFFL),
                (byte) ((high >> 24) & 0xFFL),
                (byte) ((high >> 16) & 0xFFL),
                (byte) ((high >> 8) & 0xFFL),
                (byte) ((high) & 0xFFL),
                (byte) ((low >> 56) & 0xFFL),
                (byte) ((low >> 48) & 0xFFL),
                (byte) ((low >> 40) & 0xFFL),
                (byte) ((low >> 32) & 0xFFL),
                (byte) ((low >> 24) & 0xFFL),
                (byte) ((low >> 16) & 0xFFL),
                (byte) ((low >> 8) & 0xFFL),
                (byte) ((low) & 0xFFL) };
    }

    public BigInteger toBigInteger() {
        if (isInLong()) {
            return BigInteger.valueOf(low);
        }

        return new BigInteger(toByteArray());
    }

    @Override
    public String toString() {
        if (high == 0L) {
            return Long.toUnsignedString(low);
        }

        if (isInLong()) {
            return Long.toString(low);
        }

        return toBigInteger().toString();
    }

    @Override
    public int compareTo(Int128 other) {
        int result = Long.compare(this.high, other.high);
        if (result != 0) {
            return result;
        }
        return Long.compareUnsigned(this.low, other.low);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof Int128)) {
            return false;
        }
        Int128 other = (Int128) object;
        return this.high == other.high && this.low == other.low;
    }

    @Override
    public int hashCode() {
        return Objects.hash(high, low);
    }
}
