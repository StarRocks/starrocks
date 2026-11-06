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

package com.starrocks.sql.analyzer;

/**
 * Single source of truth for the percentile-aggregate compression literal
 * contract on the FE side. Mirrors the BE constants in
 * {@code PercentileApproxAggregateFunctionBase} (be/src/exprs/agg/percentile_approx.h).
 *
 * <p>Used by {@link FunctionAnalyzer} to canonicalize the literal in the AST.
 */
public final class PercentileCompression {
    /** Lower bound of the accepted compression range. */
    public static final long MIN = 2048;

    /** Upper bound of the accepted compression range. */
    public static final long MAX = 10000;

    /**
     * Default applied when no explicit compression is given, or when a literal
     * is out of range / NULL / non-finite. Matches BE
     * {@code DEFAULT_COMPRESSION_FACTOR}.
     */
    public static final long DEFAULT = 10000;

    private PercentileCompression() {
    }
}
