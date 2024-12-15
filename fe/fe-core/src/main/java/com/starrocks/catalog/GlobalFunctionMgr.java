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
import com.starrocks.common.StarRocksException;
import com.starrocks.persist.ImageWriter;
import com.starrocks.persist.metablock.SRMetaBlockEOFException;
import com.starrocks.persist.metablock.SRMetaBlockException;
import com.starrocks.persist.metablock.SRMetaBlockID;
import com.starrocks.persist.metablock.SRMetaBlockReader;
import com.starrocks.persist.metablock.SRMetaBlockWriter;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    private void addFunction(Function function, boolean isReplay, boolean allowExists, boolean createIfNotExists)
            throws StarRocksException {
        String functionName = function.getFunctionName().getFunction();
        List<Function> existFuncs = name2Function.getOrDefault(functionName, ImmutableList.of());
        if (allowExists && createIfNotExists) {
            // In most DB system (like MySQL, Oracle, Snowflake etc.), these two conditions are now allowed to use together
            throw new StarRocksException(
                    "\"IF NOT EXISTS\" and \"OR REPLACE\" cannot be used together in the same CREATE statement");
        }
        if (!isReplay) {
            for (Function existFunc : existFuncs) {
                if (function.compare(existFunc, Function.CompareMode.IS_IDENTICAL)) {
                    if (createIfNotExists) {
                        LOG.info("create function [{}] which already exists", functionName);
                        return;
                    } else if (!allowExists) {
                        throw new StarRocksException("function already exists");
                    }
                }
            }
            assignIdToUserDefinedFunction(function);
        }
        name2Function.put(functionName, addOrReplaceFunction(function, existFuncs));
    }

    /**
     * Add the function to the given list of functions. If an identical function exists within the list, it is replaced
     * by the incoming function.
     *
     * @param function   The function to be added.
     * @param existFuncs The list of functions to which the function is added. This list is not modified.
     * @return a new list of functions with the given function added or replaced.
     */
    public static ImmutableList<Function> addOrReplaceFunction(Function function, List<Function> existFuncs) {
        return ImmutableList.<Function>builder()
                .addAll(existFuncs.stream()
                        .filter(f -> !function.compare(f, Function.CompareMode.IS_IDENTICAL))
                        .collect(ImmutableList.toImmutableList()))
                .add(function)
                .build();
    }

    /**
     * Assign a globally unique id to the given user-defined function.
     * All user-defined functions IDs are negative to avoid conflicts with the builtin function.
     *
     * @param function Function to be modified.
     */
    public static void assignIdToUserDefinedFunction(Function function) {
        long functionId = GlobalStateMgr.getCurrentState().getNextId();
        function.setFunctionId(-functionId);
    }

    public synchronized void userAddFunction(Function f, boolean allowExists, boolean createIfNotExists) throws
            StarRocksException {
        addFunction(f, false, allowExists, createIfNotExists);
        GlobalStateMgr.getCurrentState().getEditLog().logAddFunction(f);
    }

    public synchronized void replayAddFunction(Function f) {
        try {
            addFunction(f, true, false, false);
        } catch (StarRocksException e) {
            Preconditions.checkArgument(false);
        }
    }

    private void dropFunction(FunctionSearchDesc function, boolean dropIfExists) throws StarRocksException {
        String functionName = function.getName().getFunction();
        List<Function> existFuncs = name2Function.get(functionName);
        if (existFuncs == null) {
            if (dropIfExists) {
                LOG.info("drop function [{}] which does not exist", functionName);
                return;
            }
            throw new StarRocksException("Unknown function, function=" + function.toString());
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
            if (dropIfExists) {
                LOG.info("drop function [{}] which does not exist", functionName);
                return;
            }
            throw new StarRocksException("Unknown function, function=" + function.toString());
        }
        ImmutableList<Function> newFunctions = builder.build();
        if (newFunctions.isEmpty()) {
            name2Function.remove(functionName);
        } else {
            name2Function.put(functionName, newFunctions);
        }
    }

    public synchronized void userDropFunction(FunctionSearchDesc f, boolean dropIfExists) throws StarRocksException {
        dropFunction(f, dropIfExists);
        GlobalStateMgr.getCurrentState().getEditLog().logDropFunction(f);
    }

    public synchronized void replayDropFunction(FunctionSearchDesc f) {
        try {
            dropFunction(f, false);
        } catch (StarRocksException e) {
            Preconditions.checkArgument(false);
        }
    }

    public void save(ImageWriter imageWriter) throws IOException, SRMetaBlockException {
        List<Function> functions = getFunctions();

        int numJson = 1 + functions.size();
        SRMetaBlockWriter writer = imageWriter.getBlockWriter(SRMetaBlockID.GLOBAL_FUNCTION_MGR, numJson);
        writer.writeInt(functions.size());
        for (Function function : functions) {
            writer.writeJson(function);
        }

        writer.close();
    }

    public void load(SRMetaBlockReader reader) throws IOException, SRMetaBlockException, SRMetaBlockEOFException {
        reader.readCollection(Function.class, this::replayAddFunction);
    }
}

