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

package com.starrocks.sql.spm;

import com.starrocks.metric.MetricRepo;

public final class SPMMetrics {
    public static final String REWRITE_HIT = "hit";
    public static final String REWRITE_MISS = "miss";
    public static final String REWRITE_ERROR = "error";

    public static final String CAPTURE_CANDIDATE_CAPTURED = "captured";
    public static final String CAPTURE_CANDIDATE_SKIPPED_DUPLICATE = "skipped_duplicate";
    public static final String CAPTURE_CANDIDATE_SKIPPED_TABLE_COUNT = "skipped_table_count";
    public static final String CAPTURE_CANDIDATE_SKIPPED_TABLE_MISSING = "skipped_table_missing";
    public static final String CAPTURE_CANDIDATE_SKIPPED_DB_MISSING = "skipped_db_missing";
    public static final String CAPTURE_CANDIDATE_SKIPPED_PATTERN_MISMATCH = "skipped_pattern_mismatch";
    public static final String CAPTURE_CANDIDATE_FAILED = "failed";

    private SPMMetrics() {
    }

    public static void increaseRewrite(String result) {
        MetricRepo.COUNTER_SPM_REWRITE_TOTAL.getMetric(result).increase(1L);
    }

    public static void increaseCaptureCandidate(String result) {
        MetricRepo.COUNTER_SPM_CAPTURE_CANDIDATE_TOTAL.getMetric(result).increase(1L);
    }
}
