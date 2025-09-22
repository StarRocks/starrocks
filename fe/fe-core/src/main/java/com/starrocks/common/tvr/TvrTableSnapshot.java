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

package com.starrocks.common.tvr;

import java.util.Objects;
import java.util.Optional;

/**
 * TvrTableSnapshot represents a snapshot of a table at a specific version.
 */
public final class TvrTableSnapshot extends TvrVersionRange {

    public static TvrTableSnapshot empty() {
        return new TvrTableSnapshot(Optional.empty());
    }

    public static TvrTableSnapshot of(Optional<Long> end) {
        return new TvrTableSnapshot(end);
    }

    public static TvrTableSnapshot of(Long end) {
        return new TvrTableSnapshot(Optional.of(end));
    }

    public static TvrTableSnapshot of(TvrVersion end) {
        return new TvrTableSnapshot(end);
    }

    public TvrTableSnapshot(Optional<Long> snapshot) {
        this(TvrVersion.of(snapshot.orElse(TvrVersion.MIN_TIME)));
    }

    public TvrTableSnapshot(TvrVersion to) {
        super(TvrVersion.MIN, to);
    }

    @Override
    public boolean isEmpty() {
        return to.isMinOrMax();
    }

    @Override
    public String toString() {
        return "Snapshot@(" + to + ")";
    }

    @Override
    public TvrTableSnapshot copy(TvrVersion from, TvrVersion to) {
        return new TvrTableSnapshot(to);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        TvrTableSnapshot that = (TvrTableSnapshot) o;
        return Objects.equals(from, that.from) && Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(from, to);
    }
}
