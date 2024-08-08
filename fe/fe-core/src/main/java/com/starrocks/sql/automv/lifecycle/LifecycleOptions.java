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

package com.starrocks.sql.automv.lifecycle;

import com.starrocks.qe.GlobalVariable;

public class LifecycleOptions {
    private final long infantAbortionMaxTime;
    private final long initialRefreshMaxTime;
    private final long internshipPeriod;
    private final double hitRatioHwm;
    private final double hitRatioLwm;
    private final long reviveWaitingMaxTime;
    private final long performanceEvaluationInterval;
    private final long extinctionRetentionMaxTime;

    private LifecycleOptions(long infantAbortionMaxTime, long initialRefreshMaxTime, long internshipPeriod,
                             double hitRatioHwm, double hitRatioLwm, long reviveWaitingMaxTime,
                             long performanceEvaluationInterval, long extinctionRetentionMaxTime) {
        this.infantAbortionMaxTime = infantAbortionMaxTime;
        this.initialRefreshMaxTime = initialRefreshMaxTime;
        this.internshipPeriod = internshipPeriod;
        this.hitRatioHwm = hitRatioHwm;
        this.hitRatioLwm = hitRatioLwm;
        this.reviveWaitingMaxTime = reviveWaitingMaxTime;
        this.performanceEvaluationInterval = performanceEvaluationInterval;
        this.extinctionRetentionMaxTime = extinctionRetentionMaxTime;
    }

    public static LifecycleOptions getInstance() {
        return new LifecycleOptions(
                GlobalVariable.getAutoMVLifecycleInfantAbortionMaxTime(),
                GlobalVariable.getAutoMVLifecycleInitialRefreshMaxTime(),
                GlobalVariable.getAutoMVLifecycleInternshipPeriod(),
                GlobalVariable.getAutoMVLifecycleHitRatioHwm(),
                GlobalVariable.getAutoMVLifecycleHitRatioLwm(),
                GlobalVariable.getAutoMVLifecycleReviveWaitingMaxTime(),
                GlobalVariable.getAutoMVLifecyclePerformanceEvaluationInterval(),
                GlobalVariable.getAutoMVLifecycleExtinctionRetentionMaxTime());
    }

    public long getInfantAbortionMaxTime() {
        return infantAbortionMaxTime;
    }

    public long getInitialRefreshMaxTime() {
        return initialRefreshMaxTime;
    }

    public long getInternshipPeriod() {
        return internshipPeriod;
    }

    public double getHitRatioHwm() {
        return hitRatioHwm;
    }

    public double getHitRatioLwm() {
        return hitRatioLwm;
    }

    public long getReviveWaitingMaxTime() {
        return reviveWaitingMaxTime;
    }

    public long getPerformanceEvaluationInterval() {
        return performanceEvaluationInterval;
    }

    public long getExtinctionRetentionMaxTime() {
        return extinctionRetentionMaxTime;
    }
}
