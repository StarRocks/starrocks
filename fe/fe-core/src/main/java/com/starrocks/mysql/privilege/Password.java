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


package com.starrocks.mysql.privilege;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import com.starrocks.common.io.Text;
import com.starrocks.common.io.Writable;
import com.starrocks.mysql.MysqlPassword;
import com.starrocks.mysql.security.LdapSecurity;
import com.starrocks.persist.gson.GsonUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Password implements Writable {
    private static final Logger LOG = LogManager.getLogger(Password.class);

    //password is encrypted
    @SerializedName(value = "password")
    private byte[] password;
    @SerializedName(value = "authPlugin")
    private AuthPlugin authPlugin;
    @SerializedName(value = "userForAuthPlugin")
    private String userForAuthPlugin;

    public Password(byte[] password, AuthPlugin authPlugin, String userForAuthPlugin) {
        this.password = password;
        this.authPlugin = authPlugin;
        this.userForAuthPlugin = userForAuthPlugin;
    }

    public Password(byte[] password) {
        this(password, null, null);
    }

    public byte[] getPassword() {
        return password;
    }

    public void setPassword(byte[] password) {
        this.password = password;
    }

    public String getUserForAuthPlugin() {
        return userForAuthPlugin;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String jsonStr = GsonUtils.GSON.toJson(this);
        Text.writeString(out, jsonStr);
    }

    public static Password read(DataInput in) throws IOException {
        String jsonStr = Text.readString(in);
        return GsonUtils.GSON.fromJson(jsonStr, Password.class);
    }

    public boolean checkPlain(String remoteUser, String remotePassword) {
        if (remoteUser.contains(":")) {
            String[] list = remoteUser.split(":");
            remoteUser = list[list.length - 1];
        }

        if (authPlugin == null || authPlugin == AuthPlugin.MYSQL_NATIVE_PASSWORD) {
            return MysqlPassword.checkPlainPass(password, remotePassword);
        } else if (authPlugin == AuthPlugin.AUTHENTICATION_LDAP_SIMPLE) {
            if (!Strings.isNullOrEmpty(userForAuthPlugin)) {
                return LdapSecurity.checkPassword(userForAuthPlugin, remotePassword);
            } else {
                return LdapSecurity.checkPasswordByRoot(remoteUser, remotePassword);
            }
        } else {
            LOG.warn("unknown auth plugin {} to check password", authPlugin);
            return false;
        }
    }
}
