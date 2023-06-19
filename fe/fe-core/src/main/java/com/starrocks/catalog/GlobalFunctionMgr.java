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

package com.starrocks.catalog;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.starrocks.common.UserException;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * global function manager
 */
public class GlobalFunctionMgr {
    private static final Logger LOG = LogManager.getLogger(GlobalFunctionMgr.class);
    private Map<String, ImmutableList<Function>> name2Function = new HashMap<>();

    public GlobalFunctionMgr() {
    }

    public synchronized List<Function> getFunctions() {
        List<Function> functions = Lists.newArrayList();
        for (Map.Entry<String, ImmutableList<Function>> entry : name2Function.entrySet()) {
            functions.addAll(entry.getValue());
        }
        return functions;
    }

    public synchronized Function getFunction(Function desc, Function.CompareMode mode) {
        List<Function> fns = name2Function.get(desc.getFunctionName().getFunction());
        if (fns == null) {
            return null;
        }
        return Function.getFunction(fns, desc, mode);
    }

    public synchronized Function getFunction(FunctionSearchDesc function) {
        String functionName = function.getName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (existFuncs == null) {
            return null;
        }
        Function func = null;
        for (Function existFunc : existFuncs) {
            if (function.isIdentical(existFunc)) {
                func = existFunc;
                break;
            }
        }
        return func;
    }

    private void addFunction(Function function, boolean isReplay) throws UserException {
        String functionName = function.getFunctionName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (!isReplay) {
            if (existFuncs != null) {
                for (Function existFunc : existFuncs) {
                    if (function.compare(existFunc, Function.CompareMode.IS_IDENTICAL)) {
                        throw new UserException("function already exists");
                    }
                }
            }
            // Get function id for this UDF, use CatalogIdGenerator. Only get function id
            // when isReplay is false
            long functionId = GlobalStateMgr.getCurrentState().getNextId();
            // all user-defined functions id are negative to avoid conflicts with the builtin function
            function.setFunctionId(-functionId);
        }

        com.google.common.collect.ImmutableList.Builder<Function> builder =
                com.google.common.collect.ImmutableList.builder();
        if (existFuncs != null) {
            builder.addAll(existFuncs);
        }
        builder.add(function);
        name2Function.put(functionName, builder.build());
    }

    public synchronized void userAddFunction(Function f) throws UserException {
        addFunction(f, false);
        GlobalStateMgr.getCurrentState().getEditLog().logAddFunction(f);
    }

    public synchronized void replayAddFunction(Function f) {
        try {
            addFunction(f, true);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    private void dropFunction(FunctionSearchDesc function) throws UserException {
        String functionName = function.getName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (existFuncs == null) {
            throw new UserException("Unknown function, function=" + function.toString());
        }
        boolean isFound = false;
        ImmutableList.Builder<Function> builder = ImmutableList.builder();
        for (Function existFunc : existFuncs) {
            if (function.isIdentical(existFunc)) {
                isFound = true;
            } else {
                builder.add(existFunc);
            }
        }
        if (!isFound) {
            throw new UserException("Unknown function, function=" + function.toString());
        }
        ImmutableList<Function> newFunctions = builder.build();
        if (newFunctions.isEmpty()) {
            name2Function.remove(functionName);
        } else {
            name2Function.put(functionName, newFunctions);
        }
    }

    public synchronized void userDropFunction(FunctionSearchDesc f) throws UserException {
        dropFunction(f);
        GlobalStateMgr.getCurrentState().getEditLog().logDropFunction(f);
    }

    public synchronized void replayDropFunction(FunctionSearchDesc f) {
        try {
            dropFunction(f);
        } catch (UserException e) {
            Preconditions.checkArgument(false);
        }
    }

    public long loadGlobalFunctions(DataInputStream dis, long checksum) throws IOException {
        int count = dis.readInt();
        checksum ^= count;
        for (long i = 0; i < count; ++i) {
            Function f = Function.read(dis);
            replayAddFunction(f);
        }
        return checksum;
    }

    public long saveGlobalFunctions(DataOutputStream dos, long checksum) throws IOException {
        List<Function> functions = getFunctions();
        int size = functions.size();
        checksum ^= size;
        dos.writeInt(size);

        for (Function f : functions) {
            f.write(dos);
        }
        return checksum;
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        int numJson = reader.readInt();
        for (int i = 0; i < numJson; ++i) {
            Function function = reader.readJson(Function.class);
            replayAddFunction(function);
        }
    }
}

