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

package com.starrocks.load.routineload;

import com.google.common.base.Strings;

public enum TxnStatusChangeReason {

    DB_DROPPED,
    TIMEOUT,
    OFFSET_OUT_OF_RANGE,
    PAUSE,
    NO_PARTITIONS,
    FILTERED_ROWS,
    PARSE_ERROR;

    public static TxnStatusChangeReason fromString(String reasonString) {
        if (Strings.isNullOrEmpty(reasonString)) {
            return null;
        }

        for (TxnStatusChangeReason txnStatusChangeReason : TxnStatusChangeReason.values()) {
            if (reasonString.contains(txnStatusChangeReason.toString())) {
                return txnStatusChangeReason;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        switch (this) {
            case OFFSET_OUT_OF_RANGE:
                return "Offset out of range";
            case NO_PARTITIONS:
                return "No partitions have data available for loading";
            case FILTERED_ROWS:
                return "too many filtered rows";
            case PARSE_ERROR:
                return "parse error";
            default:
                return this.name();
        }
    }
}
