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

package com.starrocks.memory;

import java.util.Map;

public class MemoryStat {

    private long currentConsumption;

    private long peakConsumption;

    private String counterInfo;

    // objectName -> objectCount, Count the number of objects of a type.
    private Map<String, Long> counterMap;

    public long getCurrentConsumption() {
        return currentConsumption;
    }

    public void setCurrentConsumption(long currentConsumption) {
        this.currentConsumption = currentConsumption;
    }

    public long getPeakConsumption() {
        return peakConsumption;
    }

    public void setPeakConsumption(long peakConsumption) {
        this.peakConsumption = peakConsumption;
    }

    public String getCounterInfo() {
        return counterInfo;
    }

    public void setCounterInfo(String counterInfo) {
        this.counterInfo = counterInfo;
    }

    public Map<String, Long> getCounterMap() {
        return counterMap;
    }

    public void setCounterMap(Map<String, Long> counterMap) {
        this.counterMap = counterMap;
    }
}
