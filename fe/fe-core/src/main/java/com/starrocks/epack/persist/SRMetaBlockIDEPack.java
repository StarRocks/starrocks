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

package com.starrocks.epack.persist;

import com.starrocks.persist.metablock.SRMetaBlockID;

public class SRMetaBlockIDEPack extends SRMetaBlockID {
    private SRMetaBlockIDEPack(int id) {
        super(id);
    }

    public static final SRMetaBlockIDEPack WAREHOUSE_MGR = new SRMetaBlockIDEPack(20001);
    public static final SRMetaBlockIDEPack SECURITY_POLICY_MGR = new SRMetaBlockIDEPack(20002);
}