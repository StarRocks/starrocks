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

package com.starrocks.privilege.ranger.starrocks;

import com.starrocks.privilege.ObjectType;
import com.starrocks.privilege.ranger.RangerAccessResourceBuilder;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

public class RangerStarRocksResource extends RangerAccessResourceImpl {
    static class RangerStarRocksAccessResourceBuilder extends RangerAccessResourceBuilder {
        private RangerStarRocksAccessResourceBuilder() {
            super(new RangerStarRocksResource());
        }

        public String convertToRangerType(ObjectType objectType) {
            if (objectType.equals(ObjectType.SYSTEM)) {
                return "system";
            } else if (objectType.equals(ObjectType.USER)) {
                return "user";
            } else if (objectType.equals(ObjectType.CATALOG)) {
                return "catalog";
            } else if (objectType.equals(ObjectType.DATABASE)) {
                return "database";
            } else if (objectType.equals(ObjectType.TABLE)) {
                return "table";
            } else if (objectType.equals(ObjectType.VIEW)) {
                return "view";
            } else if (objectType.equals(ObjectType.MATERIALIZED_VIEW)) {
                return "materialized_view";
            } else if (objectType.equals(ObjectType.FUNCTION)) {
                return "function";
            } else if (objectType.equals(ObjectType.GLOBAL_FUNCTION)) {
                return "global_function";
            } else if (objectType.equals(ObjectType.RESOURCE)) {
                return "resource";
            } else if (objectType.equals(ObjectType.RESOURCE_GROUP)) {
                return "resource_group";
            } else if (objectType.equals(ObjectType.STORAGE_VOLUME)) {
                return "storage_volume";
            } else {
                return "unknown";
            }
        }
    }

    public static RangerAccessResourceBuilder builder() {
        return new RangerStarRocksAccessResourceBuilder();
    }
}