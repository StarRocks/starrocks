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


package com.starrocks.format.rest.model;

import java.io.Serializable;

public class TabletFailInfo implements Serializable {

    private static final long serialVersionUID = -9209558728067591603L;

    private Long tabletId;

    private Long backendId;

    public TabletFailInfo() {
    }

    public TabletFailInfo(Long tabletId, Long backendId) {
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
