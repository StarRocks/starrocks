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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FailPoint {
    private static final Map<String, TriggerPolicy> POLICIES = new ConcurrentHashMap<>();

    private static boolean isEnabled = false;

    public static void setTriggerPolicy(String name, TriggerPolicy triggerPolicy) {
        POLICIES.put(name, triggerPolicy);
    }

    public static void removeTriggerPolicy(String name) {
        POLICIES.remove(name);
    }

    public static boolean shouldTrigger(String name) {
        TriggerPolicy triggerPolicy = POLICIES.get(name);
        if (triggerPolicy != null) {
            return triggerPolicy.shouldTrigger();
        } else {
            return false;
        }
    }

    public static void enable() {
        isEnabled = true;
    }

    public static boolean isEnabled() {
        return isEnabled;
    }
}
