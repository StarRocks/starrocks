// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.monitor.unit;

import com.starrocks.monitor.utils.Strings;

public class ByteSizeValue implements Comparable<ByteSizeValue> {

    private final long size;
    private final ByteSizeUnit unit;

    public ByteSizeValue(long bytes) {
        this(bytes, ByteSizeUnit.BYTES);
    }

    public ByteSizeValue(long size, ByteSizeUnit unit) {
        this.size = size;
        this.unit = unit;
    }

    public int bytesAsInt() {
        long bytes = getBytes();
        if (bytes > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("size [" + toString() + "] is bigger than max int");
        }
        return (int) bytes;
    }

    public long getBytes() {
        return unit.toBytes(size);
    }

    public long getKb() {
        return unit.toKB(size);
    }

    public long getMb() {
        return unit.toMB(size);
    }

    public long getGb() {
        return unit.toGB(size);
    }

    public long getTb() {
        return unit.toTB(size);
    }

    public long getPb() {
        return unit.toPB(size);
    }

    public double getKbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C1;
    }

    public double getMbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C2;
    }

    public double getGbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C3;
    }

    public double getTbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C4;
    }

    public double getPbFrac() {
        return ((double) getBytes()) / ByteSizeUnit.C5;
    }

    @Override
    public String toString() {
        long bytes = getBytes();
        double value = bytes;
        String suffix = "B";
        if (bytes >= ByteSizeUnit.C5) {
            value = getPbFrac();
            suffix = "PB";
        } else if (bytes >= ByteSizeUnit.C4) {
            value = getTbFrac();
            suffix = "TB";
        } else if (bytes >= ByteSizeUnit.C3) {
            value = getGbFrac();
            suffix = "GB";
        } else if (bytes >= ByteSizeUnit.C2) {
            value = getMbFrac();
            suffix = "MB";
        } else if (bytes >= ByteSizeUnit.C1) {
            value = getKbFrac();
            suffix = "KB";
        }
        return Strings.format1Decimals(value, suffix);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return compareTo((ByteSizeValue) o) == 0;
    }

    @Override
    public int hashCode() {
        return Double.hashCode(((double) size) * unit.toBytes(1));
    }

    @Override
    public int compareTo(ByteSizeValue other) {
        double thisValue = ((double) size) * unit.toBytes(1);
        double otherValue = ((double) other.size) * other.unit.toBytes(1);
        return Double.compare(thisValue, otherValue);
    }
}
