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

package com.starrocks.sql.plan;

import com.google.common.base.Preconditions;

import java.math.BigInteger;

/** Immutable estimate for one execution plan, not actual usage or a tokenizer result. */
public final class AIInputTokenEstimate {
    public enum Status { NONE, ESTIMATED, UNKNOWN }

    private static final AIInputTokenEstimate NONE = new AIInputTokenEstimate(Status.NONE, BigInteger.ZERO, "");
    private final Status status;
    private final BigInteger tokens;
    private final String reason;

    private AIInputTokenEstimate(Status status, BigInteger tokens, String reason) {
        this.status = status;
        this.tokens = tokens;
        this.reason = reason;
    }

    public static AIInputTokenEstimate none() {
        return NONE;
    }

    public static AIInputTokenEstimate estimated(BigInteger tokens) {
        Preconditions.checkArgument(tokens.signum() >= 0, "negative token estimate");
        return new AIInputTokenEstimate(Status.ESTIMATED, tokens, "");
    }

    public static AIInputTokenEstimate unknown(String reason) {
        return new AIInputTokenEstimate(Status.UNKNOWN, null, reason);
    }

    public Status getStatus() {
        return status;
    }

    public BigInteger getTokens() {
        return tokens;
    }

    public String getReason() {
        return reason;
    }

    public AIInputTokenEstimate add(AIInputTokenEstimate other) {
        if (status == Status.NONE) {
            return other;
        }
        if (other.status == Status.NONE || status == Status.UNKNOWN) {
            return this;
        }
        if (other.status == Status.UNKNOWN) {
            return other;
        }
        return estimated(tokens.add(other.tokens));
    }

    @Override
    public String toString() {
        return status == Status.UNKNOWN ? "UNKNOWN (" + reason + ")" : tokens.toString();
    }
}
