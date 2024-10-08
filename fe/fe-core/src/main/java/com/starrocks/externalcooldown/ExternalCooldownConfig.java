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

package com.starrocks.externalcooldown;

import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ExternalCooldownConfig implements Writable {
    @SerializedName("target")
    private String target;

    @SerializedName("schedule")
    private String schedule;

    @SerializedName("waitSecond")
    private long waitSecond;

    public ExternalCooldownConfig(String target, String schedule, long waitSecond) {
        this.target = target;
        this.schedule = schedule;
        this.waitSecond = waitSecond;
    }

    public ExternalCooldownConfig(ExternalCooldownConfig externalCoolDownConfig) {
        target = null;
        schedule = null;
        waitSecond = 0;
        if (externalCoolDownConfig != null) {
            target = externalCoolDownConfig.target;
            schedule = externalCoolDownConfig.schedule;
            waitSecond = externalCoolDownConfig.waitSecond;
        }
    }

    public ExternalCooldownConfig() {
        this(null, null, 0);
    }

    public boolean isReadyForAutoCooldown() {
        if (target == null || target.isEmpty()) {
            return false;
        }
        if (waitSecond <= 0) {
            return false;
        }
        if (schedule == null || schedule.isEmpty()) {
            return false;
        }
        return true;
    }

    public void buildFromProperties(Map<String, String> properties) {
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET)) {
            target = properties.get(
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SCHEDULE)) {
            schedule = properties.get(
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SCHEDULE);
        }
        if (properties.containsKey(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND)) {
            waitSecond = Long.parseLong(properties.get(
                    PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND));
        }
    }

    public String getTarget() {
        return target;
    }

    public String getSchedule() {
        return schedule;
    }

    public Long getWaitSecond() {
        return waitSecond;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    public void setSchedule(String schedule) {
        this.schedule = schedule;
    }

    public void setWaitSecond(Long waitSecond) {
        this.waitSecond = waitSecond;
    }

    public Map<String, String> getProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_TARGET, target);
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_SCHEDULE, schedule);
        properties.put(PropertyAnalyzer.PROPERTIES_EXTERNAL_COOLDOWN_WAIT_SECOND, String.valueOf(waitSecond));
        return properties;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static ExternalCooldownConfig read(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), ExternalCooldownConfig.class);
    }

    @Override
    public String toString() {
        return String.format("{ target : %s,\n " +
                "schedule : %s,\n " +
                "wait second : %d }", target, schedule, waitSecond);
    }
}
