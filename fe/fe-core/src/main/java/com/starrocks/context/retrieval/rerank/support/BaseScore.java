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

package com.starrocks.context.retrieval.rerank.support;

import com.google.common.base.Strings;
import com.starrocks.context.retrieval.ContextSearchExecutor;

import java.util.Locale;

/**
 * Shared scoring helper for rerank strategies. Centralizes the two base-score modes:
 * <ul>
 *   <li>{@link Mode#VECTOR_ONLY} — {@code base = vectorScore}. Matches the upstream Python
 *       FK-greedy benchmark and is the recommended mode when callers want the graph signal to
 *       refine a pure vector ranking without text noise.</li>
 *   <li>{@link Mode#WEIGHTED_VT} — {@code base = textWeight*textScore + vectorWeight*vectorScore}.
 *       Preserves text contribution for ablations / mixed-signal experiments.</li>
 * </ul>
 */
public final class BaseScore {

    private BaseScore() {
    }

    public enum Mode {
        VECTOR_ONLY,
        WEIGHTED_VT;

        public static Mode parse(String raw) {
            if (Strings.isNullOrEmpty(raw)) {
                return VECTOR_ONLY;
            }
            try {
                return Mode.valueOf(raw.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ignored) {
                return VECTOR_ONLY;
            }
        }
    }

    public static double compute(ContextSearchExecutor.Candidate c,
                                 ContextSearchExecutor.Request req,
                                 Mode mode) {
        switch (mode) {
            case WEIGHTED_VT:
                return req.textWeight * c.textScore + req.vectorWeight * c.vectorScore;
            case VECTOR_ONLY:
            default:
                return c.vectorScore;
        }
    }
}
