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

package com.starrocks.catalog.system.sys;

import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.ScalarType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemId;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.memory.MemoryUsageTracker;
import com.starrocks.privilege.AccessDeniedException;
import com.starrocks.privilege.PrivilegeType;
import com.starrocks.sql.analyzer.Authorizer;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TFeMemoryItem;
import com.starrocks.thrift.TFeMemoryReq;
import com.starrocks.thrift.TFeMemoryRes;
import com.starrocks.thrift.TSchemaTableType;
import org.apache.thrift.TException;


public class SysFeMemoryUsage {


    public static final String NAME = "fe_memory_usage";

    public static SystemTable create() {
        return new SystemTable(SystemId.MEMORY_USAGE_ID, NAME,
                Table.TableType.SCHEMA,
                SystemTable.builder()
                        .column("module_name", ScalarType.createVarcharType(256))
                        .column("class_name", ScalarType.createVarcharType(256))
                        .column("current_consumption", ScalarType.createType(PrimitiveType.BIGINT))
                        .column("peak_consumption", ScalarType.createType(PrimitiveType.BIGINT))
                        .build(),
                TSchemaTableType.SYS_FE_MEMORY_USAGE);
    }

    public static TFeMemoryRes listFeMemoryUsage(TFeMemoryReq request) throws TException {
        TAuthInfo auth = request.getAuth_info();
        UserIdentity currentUser;
        if (auth.isSetCurrent_user_ident()) {
            currentUser = UserIdentity.fromThrift(auth.getCurrent_user_ident());
        } else {
            currentUser = UserIdentity.createAnalyzedUserIdentWithIp(auth.getUser(), auth.getUser_ip());
        }

        try {
            Authorizer.checkSystemAction(currentUser, null, PrivilegeType.OPERATE);
        } catch (AccessDeniedException e) {
            throw new TException(e.getMessage(), e);
        }

        TFeMemoryRes response = new TFeMemoryRes();

        MemoryUsageTracker.MEMORY_USAGE.forEach((moduleName, module) -> {
            if (module != null) {
                module.forEach((className, memoryStat) -> {
                    TFeMemoryItem item = new TFeMemoryItem();
                    item.setModule_name(moduleName);
                    item.setClass_name(className);
                    item.setCurrent_consumption(memoryStat.getCurrentConsumption());
                    item.setPeak_consumption(memoryStat.getPeakConsumption());
                    response.addToItems(item);
                });
            }
        });

        return response;
    }

}
