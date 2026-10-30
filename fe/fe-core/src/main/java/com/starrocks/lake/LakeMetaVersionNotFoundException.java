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

package com.starrocks.lake;

import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Raised when a BE reports {@code LAKE_META_VERSION_NOT_FOUND}: the tablet metadata object for the
 * version carried in a scan range does not exist, so the scan version captured at planning time can
 * never be read. Retrying the same fragments is pointless because the scan ranges still name that
 * version; the query has to be planned again from scratch.
 */
public class LakeMetaVersionNotFoundException extends RuntimeException {
    // Matches the fields BE puts in the status message, e.g.
    // "lake tablet metadata version not found, tablet_id=1001, partition_id=1002, version=144847: ..."
    private static final Pattern TABLET_ID_PATTERN = Pattern.compile("tablet_id=(-?\\d+)");
    private static final Pattern PARTITION_ID_PATTERN = Pattern.compile("partition_id=(-?\\d+)");
    private static final Pattern VERSION_PATTERN = Pattern.compile("version=(-?\\d+)");

    public LakeMetaVersionNotFoundException(String message) {
        super(message);
    }

    public OptionalLong getTabletId() {
        return extract(TABLET_ID_PATTERN);
    }

    public OptionalLong getPartitionId() {
        return extract(PARTITION_ID_PATTERN);
    }

    /**
     * The scan version BE failed to read, i.e. the version this query captured while planning.
     */
    public OptionalLong getScanVersion() {
        return extract(VERSION_PATTERN);
    }

    private OptionalLong extract(Pattern pattern) {
        String message = getMessage();
        if (message == null) {
            return OptionalLong.empty();
        }
        Matcher matcher = pattern.matcher(message);
        if (!matcher.find()) {
            return OptionalLong.empty();
        }
        try {
            return OptionalLong.of(Long.parseLong(matcher.group(1)));
        } catch (NumberFormatException e) {
            return OptionalLong.empty();
        }
    }
}
