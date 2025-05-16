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
package com.starrocks.failpoint;

import com.starrocks.thrift.TUpdateFailPointRequest;

public class TriggerPolicy {
    private final TriggerMode mode;
    private double probability;
    private int times;

    public static TriggerPolicy enablePolicy() {
        return new TriggerPolicy(TriggerMode.ENABLE);
    }

    public static TriggerPolicy probabilityPolicy(double probability) {
        return new TriggerPolicy(TriggerMode.PROBABILITY_ENABLE, probability);
    }

    public static TriggerPolicy timesPolicy(int times) {
        return new TriggerPolicy(TriggerMode.ENABLE_N_TIMES, times);
    }

    public TriggerPolicy(TriggerMode mode) {
        this.mode = mode;
    }

    public TriggerPolicy(TriggerMode mode, double probability) {
        this.mode = mode;
        this.probability = probability;
    }

    public TriggerPolicy(TriggerMode mode, int times) {
        this.mode = mode;
        this.times = times;
    }

    public boolean shouldTrigger() {
        if (mode == TriggerMode.ENABLE) {
            return true;
        }
        if (mode == TriggerMode.PROBABILITY_ENABLE) {
            return Math.random() < probability;
        }
        if (mode == TriggerMode.ENABLE_N_TIMES) {
            if (times > 0) {
                times--;
                return true;
            } else {
                return false;
            }
        }
        return false;
    }

    public static TriggerPolicy fromThrift(TUpdateFailPointRequest request) {
        if (request.isSetTimes()) {
            return TriggerPolicy.timesPolicy(request.getTimes());
        } else if (request.isSetProbability()) {
            return TriggerPolicy.probabilityPolicy(request.getProbability());
        } else {
            return TriggerPolicy.enablePolicy();
        }
    }
}
