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


package com.starrocks.authz.authorization;

import com.starrocks.server.GlobalStateMgr;

/**
 * This class is existed for forward compatibility so that when we add a new type of {@link PEntryObject}
 * in newer version and rollback to older version, the older version can still read the image file and edit log.
 * <p>
 * We achieve this by registering the new type as {@link ForwardCompatiblePEntryObject} in
 * {@link com.starrocks.persist.gson.GsonUtils} and remove the privilege entry in corresponding
 * {@link PrivilegeCollectionV2} when deserializing.
 */
public class ForwardCompatiblePEntryObject implements PEntryObject {
    @Override
    public boolean match(Object obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFuzzyMatching() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean validate(GlobalStateMgr globalStateMgr) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(PEntryObject obj) {
        throw new UnsupportedOperationException();
    }

    @Override
    public PEntryObject clone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int hashCode() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        throw new UnsupportedOperationException();
    }
}
