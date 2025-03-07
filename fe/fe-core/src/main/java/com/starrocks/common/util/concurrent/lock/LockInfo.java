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
package com.starrocks.common.util.concurrent.lock;

import java.util.List;

public class LockInfo {
    private final Long rid;
    private final List<LockHolder> owners;
    private final List<LockHolder> waiters;

    public LockInfo(Long rid, List<LockHolder> owners, List<LockHolder> waiters) {
        this.rid = rid;
        this.owners = owners;
        this.waiters = waiters;
    }

    public Long getRid() {
        return rid;
    }

    public List<LockHolder> getOwners() {
        return owners;
    }

    public List<LockHolder> getWaiters() {
        return waiters;
    }
}
