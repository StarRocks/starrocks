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

package com.starrocks.statistic.base;

import com.starrocks.statistic.sample.SampleInfo;

/*
 * For describe how to collect statistics on different column type
 */
public interface ColumnStats {
    long getTypeSize();

    String getColumnNameStr();

    String getQuotedColumnName();

    String getCombinedMultiColumnKey();

    String getMax();

    String getMin();

    String getCollectionSize();

    String getFullDataSize();

    String getFullNullCount();

    String getNDV();

    String getSampleDateSize(SampleInfo info);

    String getSampleNullCount(SampleInfo info);

    String getSampleNDV(SampleInfo info);

    default boolean supportData() {
        return false;
    }

    default boolean supportMeta() {
        return false;
    }
}
