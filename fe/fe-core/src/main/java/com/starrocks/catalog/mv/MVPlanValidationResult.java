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

package com.starrocks.catalog.mv;

/**
 * MVPlanValidationResult is used to represent the validation result of a MV's defined query plan.
 */
public class MVPlanValidationResult {
    public enum Status {
        VALID,
        INVALID,
        UNKNOWN;

        public boolean isValid() {
            return this == VALID;
        }

        public boolean isInvalid() {
            return this == INVALID;
        }

        public boolean isUnKnown() {
            return this == UNKNOWN;
        }
    }

    private final Status status;
    private final String reason;

    public MVPlanValidationResult(Status status, String message) {
        this.status = status;
        this.reason = message != null ? message : "";
    }

    public Status getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    public boolean isValid() {
        return status.isValid();
    }

    public static MVPlanValidationResult valid() {
        return new MVPlanValidationResult(Status.VALID, null);
    }

    public static MVPlanValidationResult invalid(String message) {
        return new MVPlanValidationResult(Status.INVALID, message);
    }

    public static MVPlanValidationResult unknown(String message) {
        return new MVPlanValidationResult(Status.UNKNOWN, message);
    }

    @Override
    public String toString() {
        return "MVPlanValidationResult{" +
                "status=" + status +
                ", reason='" + reason + '\'' +
                '}';
    }
}