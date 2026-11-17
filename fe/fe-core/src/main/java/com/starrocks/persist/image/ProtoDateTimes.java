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

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

/** The UTC epoch-second representation used by the v2 LocalDateTime GSON adapter. */
public final class ProtoDateTimes {
    private ProtoDateTimes() {
    }

    public static long toEpochSecond(LocalDateTime value) {
        return value.toEpochSecond(ZoneOffset.UTC);
    }

    public static LocalDateTime fromEpochSecond(long value) {
        try {
            return LocalDateTime.ofEpochSecond(value, 0, ZoneOffset.UTC);
        } catch (DateTimeException e) {
            throw new IllegalArgumentException("invalid LocalDateTime epoch second " + value, e);
        }
    }
}
