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

import com.google.common.base.Strings;
import com.starrocks.analysis.ParseNode;
import com.starrocks.sql.parser.NodePosition;

import java.util.Objects;

public class User implements ParseNode {
    public static final User ROOT;

    static {
        ROOT = new User("root", "%");
    }

    private final String user;
    private final String host;
    private final boolean isDomain;
    private final NodePosition pos;

    public User(String user, String host) {
        this(user, host, false);
    }

    public User(String user, String host, boolean isDomain) {
        this(user, host, isDomain, NodePosition.ZERO);
    }

    public User(String user, String host, boolean isDomain, NodePosition pos) {
        this.user = user;
        this.host = Strings.emptyToNull(host);
        this.isDomain = isDomain;
        this.pos = pos;
    }

    public String getUser() {
        return user;
    }

    public String getHost() {
        return host;
    }

    public boolean isDomain() {
        return isDomain;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("'");
        if (!Strings.isNullOrEmpty(user)) {
            sb.append(user);
        }
        sb.append("'@");
        if (!Strings.isNullOrEmpty(host)) {
            if (isDomain) {
                sb.append("['").append(host).append("']");
            } else {
                sb.append("'").append(host).append("'");
            }
        } else {
            sb.append("%");
        }
        return sb.toString();
    }

    @Override
    public NodePosition getPos() {
        return pos;
    }

    @Override
    public boolean equals(Object object) {
        if (object == null || getClass() != object.getClass()) {
            return false;
        }
        User user1 = (User) object;
        return isDomain == user1.isDomain && Objects.equals(user, user1.user) && Objects.equals(host, user1.host);
    }

    @Override
    public int hashCode() {
        return Objects.hash(user, host, isDomain);
    }
}
