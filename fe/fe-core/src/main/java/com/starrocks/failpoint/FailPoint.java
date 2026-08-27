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
        TriggerPolicy previous = POLICIES.put(name, triggerPolicy);
        if (previous != null) {
            // Re-arming must not strand threads parked on the policy being replaced.
            previous.release();
        }
    }

    public static void removeTriggerPolicy(String name) {
        TriggerPolicy removed = POLICIES.remove(name);
        if (removed != null) {
            // ADMIN DISABLE FAILPOINT is the release command for a PAUSE policy.
            removed.release();
        }
    }

    /**
     * Remove {@code name} only if it is still mapped to {@code expected}, then release it. Used by a
     * timed-out pause to disarm itself: an unconditional remove would delete a policy that another
     * thread installed for the same name in the meantime, silently discarding the operator's new mode.
     *
     * @return true if this call removed the policy
     */
    public static boolean removeTriggerPolicyIf(String name, TriggerPolicy expected) {
        // ConcurrentHashMap.remove(key, value) compares with equals(), which TriggerPolicy does not
        // override, so this is an identity check -- exactly what is wanted here.
        boolean removed = POLICIES.remove(name, expected);
        if (removed) {
            expected.release();
        }
        return removed;
    }

    public static boolean shouldTrigger(String name) {
        TriggerPolicy triggerPolicy = POLICIES.get(name);
        if (triggerPolicy != null) {
            return triggerPolicy.shouldTrigger(name);
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
