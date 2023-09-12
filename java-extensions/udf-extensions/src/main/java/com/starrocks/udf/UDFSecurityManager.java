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

package com.starrocks.udf;

import java.security.Permission;

public class UDFSecurityManager extends SecurityManager {
    private Class<?> clazz;

    public UDFSecurityManager(Class<?> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void checkPermission(Permission perm) {
        if (isCreateFromUDFClassLoader()) {
            super.checkPermission(perm);
        }
    }

    public void checkPermission(Permission perm, Object context) {
        if (isCreateFromUDFClassLoader()) {
            super.checkPermission(perm, context);
        }
    }

    private boolean isCreateFromUDFClassLoader() {
        Class<?>[] classContext = getClassContext();
        if (classContext.length >= 2) {
            for (int i = 1; i < classContext.length; i++) {
                if (classContext[i].getClassLoader() != null &&
                        clazz.equals(classContext[i].getClassLoader().getClass())) {
                    return true;
                }
            }
        }
        return false;
    }
}
