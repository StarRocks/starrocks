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

package com.starrocks.persist.image;

import java.util.Objects;

/**
 * Identifies one section of a format-v3 image. The numeric value is what goes into
 * {@code IndexEntryPB.type}; the name only serves logging.
 *
 * <p>Values are assigned from per-module ranges and are never reused. The open-source
 * range is [1, 20000); enterprise sections extend this class and use [20001, ...), the
 * same convention as {@code SRMetaBlockID}. The value space is independent of
 * {@code SRMetaBlockID}: a v2 block may map to several v3 sections.
 */
public class SectionId {
    public static final SectionId INVALID = new SectionId(0, "INVALID");

    private final int value;
    private final String name;

    protected SectionId(int value, String name) {
        this.value = value;
        this.name = Objects.requireNonNull(name, "name");
    }

    public int getValue() {
        return value;
    }

    public String getName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SectionId && ((SectionId) o).value == value;
    }

    @Override
    public int hashCode() {
        return value;
    }

    @Override
    public String toString() {
        return name + "(" + value + ")";
    }
}
