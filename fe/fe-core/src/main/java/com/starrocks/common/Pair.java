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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/main/java/org/apache/doris/common/Pair.java

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

package com.starrocks.common;

import com.google.gson.annotations.SerializedName;

import java.util.Comparator;

/**
 * The equivalent of C++'s std::pair<>.
 * <p>
 * Notice: When using Pair for persistence, users need to guarantee that F and S can be serialized through Gson
 */
public class Pair<F, S> {

    @SerializedName(value = "first")
    public F first;
    @SerializedName(value = "second")
    public S second;

    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    public static <F, S> Pair<F, S> create(F first, S second) {
        return new Pair<F, S>(first, second);
    }

    @Override
    /*
     * A pair is equal if both parts are equal().
     */
    public boolean equals(Object o) {
        if (!(o instanceof Pair)) {
            return false;
        }
        Pair<F, S> other = (Pair<F, S>) o;

        // compare first
        if (this.first == null) {
            if (other.first != null) {
                return false;
            }
        } else {
            if (! this.first.equals(other.first)) {
                return false;
            }
        }

        // compare second
        if (this.second == null) {
            return other.second == null;
        } else {
            return this.second.equals(other.second);
        }
    }

    @Override
    public int hashCode() {
        int hashFirst = first != null ? first.hashCode() : 0;
        int hashSecond = second != null ? second.hashCode() : 0;

        return (hashFirst + hashSecond) * hashSecond + hashFirst;
    }

    @Override
    public String toString() {
        return first.toString() + ":" + second.toString();
    }

    public static <K, V extends Comparable<? super V>> Comparator<Pair<K, V>> comparingBySecond() {
        return Comparator.comparing(c -> c.second);
    }
}
