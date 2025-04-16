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
package com.starrocks.encryption;

import com.starrocks.proto.EncryptionKeyPB;

public abstract class EncryptionKey {
    protected long id = 0; // 0 means id is not allocated for this key
    protected EncryptionKey parent = null; // 0 means it is a root/master key
    protected long createTime = 0;

    public EncryptionKey() {
        createTime = System.currentTimeMillis() / 1000;
    }

    public EncryptionKey getParent() {
        return parent;
    }

    public void setParent(EncryptionKey parent) {
        this.parent = parent;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public long getCreateTime() {
        return createTime;
    }

    public boolean isMasterKey() {
        return id == KeyMgr.DEFAULT_MASTER_KYE_ID;
    }

    public boolean isKEK() {
        return parent != null && parent.isMasterKey();
    }

    public abstract EncryptionKey generateKey();

    public abstract void decryptKey(EncryptionKey key);

    public void toPB(EncryptionKeyPB pb, KeyMgr mgr) {
        if (id != 0) {
            pb.id = id;
        }
        if (parent != null && parent.id != 0) {
            pb.parentId = parent.id;
        }
        if (createTime != 0) {
            pb.createTime = createTime;
        }
    }

    public void fromPB(EncryptionKeyPB pb, KeyMgr mgr) {
        if (pb.id != null) {
            id = pb.id;
        }
        if (pb.parentId != null) {
            EncryptionKey parent = mgr.getKeyById(pb.parentId);
            if (parent != null) {
                this.parent = parent;
            } else {
                throw new IllegalArgumentException(
                        String.format("parent key with id:%s not found for key with id:%s", pb.parentId, id));
            }
        }
        if (pb.createTime != null) {
            createTime = pb.createTime;
        }
    }

    public abstract String toSpec();

    public static EncryptionKey createFromSpec(String spec) {
        if (spec == null || !spec.contains(":")) {
            throw new IllegalArgumentException("Invalid spec format");
        }
        String[] parts = spec.split(":", 2);
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid spec format");
        }
        String type = parts[0];
        String keySpec = parts[1];
        if (type.equalsIgnoreCase("plain")) {
            return NormalKey.createFromSpec(keySpec);
        } else {
            throw new IllegalArgumentException("key spec type not supported: " + type);
        }
    }
}
