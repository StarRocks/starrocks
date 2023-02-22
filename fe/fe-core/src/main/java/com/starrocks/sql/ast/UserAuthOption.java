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
import com.starrocks.sql.parser.NodePosition;

public class UserAuthOption implements ParseNode {
    private final String password;
    private final String authPlugin;
    private final String authString;
    private final boolean passwordPlain;

    private final NodePosition pos;

    public UserAuthOption(String password, String authPlugin, String authString, boolean passwordPlain) {
        this(password, authPlugin, authString, passwordPlain, NodePosition.ZERO);
    }

    public UserAuthOption(String password, String authPlugin, String authString, boolean passwordPlain,
                          NodePosition pos) {
        this.pos = pos;
        this.password = password;
        this.authPlugin = authPlugin;
        this.authString = authString;
        this.passwordPlain = passwordPlain;
    }

    public String getPassword() {
        return password;
    }

    public String getAuthPlugin() {
        return authPlugin;
    }

    public String getAuthString() {
        return authString;
    }

    public boolean isPasswordPlain() {
        return passwordPlain;
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }
}
