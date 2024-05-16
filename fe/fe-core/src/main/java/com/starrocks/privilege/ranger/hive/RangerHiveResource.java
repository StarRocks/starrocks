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

package com.starrocks.privilege.ranger.hive;

import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.ranger.RangerAccessResourceBuilder;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

public class RangerHiveResource extends RangerAccessResourceImpl {
    static class RangerHiveAccessResourceBuilder extends RangerAccessResourceBuilder {
        private RangerHiveAccessResourceBuilder() {
            super(new RangerHiveResource());
        }

        @Override
        public String convertToRangerType(ObjectType objectType) {
            if (objectType.equals(ObjectType.DATABASE)) {
                return "database";
            } else if (objectType.equals(ObjectType.TABLE)) {
                return "table";
            } else {
                return "unknown";
            }
        }
    }

    public static RangerAccessResourceBuilder builder() {
        return new RangerHiveAccessResourceBuilder();
    }
}
