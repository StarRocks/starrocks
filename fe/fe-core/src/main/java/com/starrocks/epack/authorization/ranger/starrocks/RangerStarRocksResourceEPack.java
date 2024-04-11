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
package com.starrocks.epack.authorization.ranger.starrocks;

import com.starrocks.epack.authorization.ObjectTypeEPack;
import com.starrocks.epack.sql.ast.PolicyType;
import com.starrocks.privilege.ObjectType;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

public class RangerStarRocksResourceEPack extends RangerAccessResourceImpl {

    public static RangerStarRocksResourceEPack makePolicyResource(PolicyType policyType, String catalogName,
                                                                  String db, String policy) {
        RangerStarRocksResourceEPack resourceEPack = new RangerStarRocksResourceEPack();
        if (policyType.equals(PolicyType.MASKING)) {
            resourceEPack.setValue(convertToRangerType(ObjectType.CATALOG), catalogName);
            resourceEPack.setValue(convertToRangerType(ObjectType.DATABASE), db);
            resourceEPack.setValue(convertToRangerType(ObjectTypeEPack.MASKING_POLICY), policy);
        } else {
            resourceEPack.setValue(convertToRangerType(ObjectType.CATALOG), catalogName);
            resourceEPack.setValue(convertToRangerType(ObjectType.DATABASE), db);
            resourceEPack.setValue(convertToRangerType(ObjectTypeEPack.ROW_ACCESS_POLICY), policy);
        }

        return resourceEPack;
    }

    private static String convertToRangerType(ObjectType objectType) {
        if (objectType.equals(ObjectTypeEPack.MASKING_POLICY)) {
            return "masking_policy";
        } else if (objectType.equals(ObjectTypeEPack.ROW_ACCESS_POLICY)) {
            return "row_access_policy";
        } else {
            return "unknown";
        }
    }
}
