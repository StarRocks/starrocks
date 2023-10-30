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

package com.starrocks.connector.odps;

import com.aliyun.odps.table.read.TableBatchReadSession;
import com.aliyun.odps.table.read.split.InputSplit;
import com.starrocks.connector.exception.StarRocksConnectorException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;
import java.util.List;

/**
 * @author dingxin (zhangdingxin.zdx@alibaba-inc.com)
 */
public class OdpsSplitsInfo {
    private final List<InputSplit> splits;

    private final TableBatchReadSession session;

    public OdpsSplitsInfo(List<InputSplit> splits, TableBatchReadSession session) {
        this.splits = splits;
        this.session = session;
    }

    public boolean isEmpty() {
        return splits.isEmpty();
    }

    public List<InputSplit> getSplits() {
        return splits;
    }

    public TableBatchReadSession getSession() {
        return session;
    }

    public String getSerializeSession() {
        try {
            return serialize(session);
        } catch (IOException e) {
            throw new StarRocksConnectorException("Serialize odps read session failed", e);
        }
    }

    private String serialize(Serializable object) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
        objectOutputStream.writeObject(object);
        byte[] serializedBytes = byteArrayOutputStream.toByteArray();
        return Base64.getEncoder().encodeToString(serializedBytes);
    }
}
