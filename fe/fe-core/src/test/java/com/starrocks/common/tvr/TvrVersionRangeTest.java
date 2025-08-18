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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class TvrVersionRangeTest {

    @Test
    public void isEmptyReturnsTrueForMinEndMaxRange() {
        TvrVersion minVersion = TvrVersion.MIN;
        TvrVersion maxVersion = TvrVersion.MAX;
        TvrVersionRange range = new TvrVersionRange(minVersion, maxVersion) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };

        Assertions.assertTrue(range.isEmpty());
    }

    @Test
    public void isEmptyReturnsTrueForEqualStartAndEnd() {
        TvrVersion version = new TvrVersion(1L);
        TvrVersionRange range = new TvrVersionRange(version, version) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };

        Assertions.assertTrue(range.isEmpty());
    }

    @Test
    public void isEmptyReturnsFalseForNonEmptyRange() {
        TvrVersion from = new TvrVersion(1L);
        TvrVersion to = new TvrVersion(2L);
        TvrVersionRange range = new TvrVersionRange(from, to) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };

        Assertions.assertFalse(range.isEmpty());
    }

    @Test
    public void startReturnsEmptyForMinVersion() {
        TvrVersion minVersion = TvrVersion.MIN;
        TvrVersionRange range = new TvrVersionRange(minVersion, TvrVersion.MAX) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };

        Assertions.assertEquals(Optional.empty(), range.start());
    }

    @Test
    public void startReturnsVersionForNonMinVersion() {
        TvrVersion version = new TvrVersion(1L);
        TvrVersionRange range = new TvrVersionRange(version, TvrVersion.MAX) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };

        Assertions.assertEquals(Optional.of(1L), range.start());
    }

    @Test
    public void endReturnsEmptyForMaxVersion() {
        TvrVersion maxVersion = TvrVersion.MAX;
        TvrVersionRange range = new TvrVersionRange(TvrVersion.MIN, maxVersion) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };

        Assertions.assertEquals(Optional.empty(), range.end());
    }

    @Test
    public void endReturnsVersionForNonMaxVersion() {
        TvrVersion version = new TvrVersion(2L);
        TvrVersionRange range = new TvrVersionRange(TvrVersion.MIN, version) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };

        Assertions.assertEquals(Optional.of(2L), range.end());
    }

    @Test
    public void timeRangeEqualsReturnsTrueForEqualRanges() {
        TvrVersion from = new TvrVersion(1L);
        TvrVersion to = new TvrVersion(2L);
        TvrVersionRange range1 = new TvrVersionRange(from, to) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };
        TvrVersionRange range2 = new TvrVersionRange(from, to) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };

        Assertions.assertTrue(range1.timeRangeEquals(range2));
    }

    @Test
    public void timeRangeEqualsReturnsFalseForDifferentRanges() {
        TvrVersion from1 = new TvrVersion(1L);
        TvrVersion to1 = new TvrVersion(2L);
        TvrVersion from2 = new TvrVersion(3L);
        TvrVersion to2 = new TvrVersion(4L);
        TvrVersionRange range1 = new TvrVersionRange(from1, to1) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };
        TvrVersionRange range2 = new TvrVersionRange(from2, to2) {
            @Override
            public TvrVersionRange copy(TvrVersion from, TvrVersion to) {
                return null;
            }
        };

        Assertions.assertFalse(range1.timeRangeEquals(range2));
    }
}