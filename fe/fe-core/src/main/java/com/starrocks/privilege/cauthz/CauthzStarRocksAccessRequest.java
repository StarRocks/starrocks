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

package com.starrocks.privilege.cauthz;

import com.starrocks.privilege.cauthz.CauthzAccessResourceImpl;
import com.starrocks.sql.ast.UserIdentity;

/**
 * CauthzStarRocksAccessRequest is an object that is used to represent a single authorization request
 * A single authorization request is composed of a user, a resource and an access type.
 */
public class CauthzStarRocksAccessRequest {
    private String user;
    private CauthzAccessResourceImpl resource;
    private String accessType;


    public CauthzStarRocksAccessRequest(CauthzAccessResourceImpl resource, UserIdentity user, String accessType) {
        setResource(resource);
        setUser(user);
        setAccessType(accessType);
    }

    public String getUser() {
        return user;
    }

    public CauthzAccessResourceImpl getResource() {
        return resource;
    }

    public String getAccessType() {
        return accessType;
    }

    public void setUser(UserIdentity user) {
        this.user = user.getUser();
    }

    public void setResource(CauthzAccessResourceImpl resource) {
        this.resource = resource;
    }

    public void setAccessType(String accessType) {
        this.accessType = accessType;
    }

    public String toString() {
        return "CauthzStarRocksAccessRequest{" +
                "user=" + user +
                ", resource=" + resource +
                ", accessType=" + accessType +
                '}';
    }
} 