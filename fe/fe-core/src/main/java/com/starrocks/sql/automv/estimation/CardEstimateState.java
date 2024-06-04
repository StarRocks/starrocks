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

package com.starrocks.sql.automv.estimation;

import com.starrocks.sql.automv.options.AutoMVOptions;

public class CardEstimateState {
    private final AutoMVOptions options;
    private final HaltReason haltReason;
    private final long timeUsage;
    private final double samplingRatio;
    private final long calcSteps;
    private final CardQuality cardQuality;

    public CardEstimateState(AutoMVOptions options, HaltReason haltReason, long timeUsage, double samplingRatio,
                             long calcSteps) {
        this.options = options;
        this.haltReason = haltReason;
        this.timeUsage = timeUsage;
        this.samplingRatio = samplingRatio;
        this.calcSteps = calcSteps;
        this.cardQuality = estimateCardQuality(options, haltReason, samplingRatio);
    }

    private static CardQuality estimateCardQuality(AutoMVOptions options, HaltReason haltReason,
                                                   double samplingRatio) {
        if (haltReason == HaltReason.OVERALL) {
            return CardQuality.EXCELLENT;
        } else if (haltReason == HaltReason.CONVERGENT) {
            if (samplingRatio >= 0.6) {
                return CardQuality.EXCELLENT;
            } else if (samplingRatio >= 0.4) {
                return CardQuality.GOOD;
            } else {
                return CardQuality.PASS;
            }
        } else {
            if (samplingRatio >= 0.6) {
                return CardQuality.GOOD;
            } else if (samplingRatio >= 0.4) {
                return CardQuality.PASS;
            } else {
                return CardQuality.FAIL;
            }
        }
    }

    public AutoMVOptions getOptions() {
        return options;
    }

    public HaltReason getHaltReason() {
        return haltReason;
    }

    public long getTimeUsage() {
        return timeUsage;
    }

    public double getSamplingRatio() {
        return samplingRatio;
    }

    public long getCalcSteps() {
        return calcSteps;
    }

    public CardQuality getCardQuality() {
        return cardQuality;
    }

    public enum HaltReason {
        ERROR,
        TIMEOUT,
        REACH_LIMIT,
        CONVERGENT,
        OVERALL,
    }

    public enum CardQuality {
        EXCELLENT,
        GOOD,
        PASS,
        FAIL,
    }
}
