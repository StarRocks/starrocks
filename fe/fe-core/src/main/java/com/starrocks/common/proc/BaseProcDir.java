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

package com.starrocks.common.proc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.AnalysisException;

import java.util.Map;

// Generic PROC DIR class that can be registered and return the content of the underlying node.
// Non-thread-safe, requires the caller to consider thread-safe content.
public class BaseProcDir implements ProcDirInterface {
    protected Map<String, ProcNodeInterface> nodeMap;

    public BaseProcDir() {
        nodeMap = Maps.newConcurrentMap();
    }

    @Override
    public boolean register(String name, ProcNodeInterface node) {
        nodeMap.put(name, node);
        return true;
    }

    @Override
    public ProcNodeInterface lookup(String name) {
        return nodeMap.get(name);
    }

    @Override
    public ProcResult fetchResult() throws AnalysisException {
        BaseProcResult result = new BaseProcResult();
        result.setNames(Lists.newArrayList("name"));
        for (String name : nodeMap.keySet()) {
            result.addRow(Lists.newArrayList(name));
        }
        return result;
    }
}
