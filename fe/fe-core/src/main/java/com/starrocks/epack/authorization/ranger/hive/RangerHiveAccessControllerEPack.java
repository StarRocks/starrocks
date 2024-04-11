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
package com.starrocks.epack.authorization.ranger.hive;

import com.starrocks.epack.authorization.AccessControllerEPack;
import com.starrocks.epack.authorization.ranger.RangerKerberosAuth;
import com.starrocks.privilege.ranger.hive.RangerHiveAccessController;
import org.apache.ranger.authorization.hadoop.config.RangerPluginConfig;

public class RangerHiveAccessControllerEPack extends RangerHiveAccessController implements AccessControllerEPack {
    public RangerHiveAccessControllerEPack(String serviceName) {
        super(serviceName);
    }

    @Override
    protected RangerPluginConfig buildRangerPluginContext(String serviceType, String serviceName) {
        return RangerKerberosAuth.buildKerberosRangerPluginContext(serviceType, serviceName);
    }
}
