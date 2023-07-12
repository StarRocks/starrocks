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

package com.starrocks.qe;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.starrocks.thrift.TScanRangeParams;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * map from an backend host address to the per-node assigned scan ranges;
 * records scan range assignment for a single fragment
 */
class FragmentScanRangeAssignment extends
        HashMap<Long, Map<Integer, List<TScanRangeParams>>> {
    public String toDebugString() {
        StringBuilder sb = new StringBuilder();
        sb.append("---------- FragmentScanRangeAssignment ----------\n");
        for (Long workerId : keySet()) {
            Map<Integer, List<TScanRangeParams>> placement = get(workerId);
            for (Integer scanNodeId : placement.keySet()) {
                ArrayList<TScanRangeParams> scanRangeParams = new ArrayList<>(placement.get(scanNodeId));
                Collections.sort(scanRangeParams);
                TMemoryBuffer transport = new TMemoryBuffer(1024 * 1024);
                TBinaryProtocol protocol = new TBinaryProtocol(transport);
                String output;
                try {
                    for (TScanRangeParams param : scanRangeParams) {
                        param.write(protocol);
                    }
                    HashFunction f = Hashing.murmur3_128();
                    HashCode code = f.hashBytes(transport.getArray());
                    output = code.toString();
                } catch (TException e) {
                    output = e.toString();
                }
                sb.append(String.format("Backend:%s, ScanNode:%s, Hash:%s\n", workerId, scanNodeId.toString(), output));

            }
        }
        return sb.toString();
    }
}