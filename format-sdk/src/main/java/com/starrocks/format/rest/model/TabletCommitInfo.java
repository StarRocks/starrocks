// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.format.rest.model;


import java.io.Serializable;

public class TabletCommitInfo implements Serializable {

    private static final long serialVersionUID = 8431256353347497743L;

    private Long tabletId;

    private Long backendId;

    public TabletCommitInfo() {
    }

    public TabletCommitInfo(Long tabletId, Long backendId) {
        this.tabletId = tabletId;
        this.backendId = backendId;
    }

    public Long getTabletId() {
        return tabletId;
    }

    public void setTabletId(Long tabletId) {
        this.tabletId = tabletId;
    }

    public Long getBackendId() {
        return backendId;
    }

    public void setBackendId(Long backendId) {
        this.backendId = backendId;
    }
}
