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

/**
 * BM25-lite term scoring for the context-search text channel.
 *
 * <p>This is BM25 without the IDF factor: the FE text fold has the per-entity match count and the
 * matched fragments' {@code token_count}, but not per-term document frequency, so the IDF term is
 * omitted (it would require BE inverted-index term statistics — a deferred follow-up). What remains
 * is the part that fixes the two worst pathologies of the legacy {@code min(1, hit_count/10)}
 * count:
 *
 * <ul>
 *   <li><b>TF saturation</b> (parameter {@code k1}): repeating a term has diminishing returns, so a
 *       keyword-stuffed document cannot dominate by raw count.</li>
 *   <li><b>Length normalization</b> (parameter {@code b}): longer documents are penalized relative
 *       to {@code avgdl}, removing the "longer text matches more by accident" advantage.</li>
 * </ul>
 *
 * <p>Score = {@code tf*(k1+1) / (tf + k1*(1 - b + b*dl/avgdl))}, then divided by {@code (k1+1)} so
 * the result lands in {@code [0,1]} (keeps it commensurate with the vector channel for the additive
 * strategy; the RRF strategy only uses the induced ordering, which the {@code (k1+1)} constant does
 * not change).
 *
 * <p>The same formula is emitted as SQL by {@link #scoreSql} so the in-SQL text fold and this pure
 * Java reference stay byte-for-byte consistent and unit-testable.
 */
public final class Bm25 {

    /** TF-saturation parameter k1 (standard range 1.2-2.0). */
    public static final double DEFAULT_K1 = 1.2;
    /** Length-normalization parameter b in [0,1] (0 = no length norm; standard 0.75). */
    public static final double DEFAULT_B = 0.75;
    /** Average document length (tokens) used as the length-norm denominator. A shared constant, so
     *  it only shifts the length-penalty operating point, not the relative ranking. */
    public static final double DEFAULT_AVGDL = 160.0;

    private Bm25() {
    }

    /** {@link #scoreSql(String, String, double, double, double)} with the default k1/b/avgdl. */
    public static String scoreSql(String tfExpr, String dlExpr) {
        return scoreSql(tfExpr, dlExpr, DEFAULT_K1, DEFAULT_B, DEFAULT_AVGDL);
    }

    /**
     * Pure numeric BM25-lite score. {@code tf} is the match count (term frequency proxy), {@code dl}
     * the document length in tokens, {@code avgdl} the average document length. A non-positive
     * {@code dl} is treated as {@code avgdl} (neutral length factor); {@code avgdl <= 0} disables
     * length normalization.
     */
    public static double score(double tf, double dl, double avgdl, double k1, double b) {
        if (tf <= 0.0) {
            return 0.0;
        }
        double effectiveAvgdl = avgdl > 0.0 ? avgdl : 1.0;
        double effectiveDl = dl > 0.0 ? dl : effectiveAvgdl;
        double denom = tf + k1 * (1.0 - b + b * (effectiveDl / effectiveAvgdl));
        double raw = (tf * (k1 + 1.0)) / denom;
        return raw / (k1 + 1.0);
    }

    /**
     * Inverse document frequency for a query term, in the Lucene/Milvus form
     * {@code ln((N - df + 0.5) / (df + 0.5) + 1)}. The trailing {@code + 1} keeps the argument
     * {@code >= 1} for any {@code 0 <= df <= N}, so the result is always non-negative (a term in
     * almost every document contributes ~0, never a negative score). This is the IDF factor the
     * legacy FE text fold could not compute; on the builtin GIN path {@code df} is the number of
     * distinct entities a term matches (exact, corpus-wide) and {@code n} the corpus entity count.
     */
    public static double idf(long n, long df) {
        if (n <= 0L) {
            return 0.0;
        }
        double clampedDf = df < 0L ? 0.0 : (df > n ? n : df);
        return Math.log((n - clampedDf + 0.5) / (clampedDf + 0.5) + 1.0);
    }

    /**
     * One query term's BM25 contribution: {@code idf * tf*(k1+1) / (tf + k1*(1 - b + b*dl/avgdl))}.
     * Unlike {@link #score} this keeps the full {@code (k1+1)} numerator (no {@code [0,1]} rescale)
     * and multiplies in the IDF, so the sum of {@code termScore} over a query's terms is the real
     * BM25 score. {@code tf} is the term's per-document frequency (matched-fragment count proxy on
     * the builtin path), {@code dl} the document length in tokens, {@code avgdl} the average. A
     * non-positive {@code tf} contributes 0; {@code dl <= 0} collapses to the neutral {@code avgdl}
     * factor; {@code avgdl <= 0} disables length normalization.
     */
    public static double termScore(double idf, double tf, double dl, double avgdl, double k1, double b) {
        if (tf <= 0.0 || idf <= 0.0) {
            return 0.0;
        }
        double effectiveAvgdl = avgdl > 0.0 ? avgdl : 1.0;
        double effectiveDl = dl > 0.0 ? dl : effectiveAvgdl;
        double denom = tf + k1 * (1.0 - b + b * (effectiveDl / effectiveAvgdl));
        return idf * (tf * (k1 + 1.0)) / denom;
    }

    /**
     * SQL expression computing the same score as {@link #score}, given SQL expressions for the term
     * frequency and document length. Constants ({@code k1}, {@code b}, {@code avgdl}) are inlined as
     * literals. {@code dlExpr} is guarded with {@code COALESCE(NULLIF(dl,0), avgdl)} so a null/zero
     * length collapses to the neutral {@code avgdl} factor, matching {@link #score}.
     */
    public static String scoreSql(String tfExpr, String dlExpr, double k1, double b, double avgdl) {
        double effectiveAvgdl = avgdl > 0.0 ? avgdl : 1.0;
        String dlGuarded = "COALESCE(NULLIF(" + dlExpr + ", 0), " + effectiveAvgdl + ")";
        String denom = "(" + tfExpr + ") + " + k1 + " * (1.0 - " + b + " + " + b
                + " * (" + dlGuarded + " / " + effectiveAvgdl + "))";
        return "((" + tfExpr + ") * (" + k1 + " + 1.0)) / (" + denom + ") / (" + k1 + " + 1.0)";
    }
}
