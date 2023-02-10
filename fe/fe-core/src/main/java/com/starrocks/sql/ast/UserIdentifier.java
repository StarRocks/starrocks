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

package com.starrocks.sql.ast;

import com.starrocks.analysis.ParseNode;

/**
 * TODO this class is merely a wrapper of UserIdentity,
 * they should be merged after all statement migrate to the new framework
 */
public class UserIdentifier implements ParseNode {
    private final UserIdentity userIdentity;

    public UserIdentifier(String name, String host, boolean isDomain) {
        userIdentity = new UserIdentity(name, host, isDomain);
    }

    public UserIdentity getUserIdentity() {
        return userIdentity;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return null;
    }
}
