//  Copyright 2021-present StarRocks, Inc. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package com.starrocks.load.pipe.filelist;

import com.starrocks.load.pipe.PipeFileRecord;
import com.starrocks.load.pipe.PipeId;

import java.util.List;

/**
 * Store and retrieve file-list for a pipe
 */
public abstract class FileListRepo {

    protected PipeId pipeId;

    public void setPipeId(PipeId pipeId) {
        this.pipeId = pipeId;
    }

    public static FileListTableRepo createTableBasedRepo() {
        return new FileListTableRepo();
    }

    /**
     * List unloaded files, then put them into loading
     */
    public abstract List<PipeFileRecord> listUnloadedFiles();

    /**
     * Add files into the list, as unloaded state
     * If some files have already been loaded, they will not been added
     */
    public abstract void addFiles(List<PipeFileRecord> files);

    /**
     * Update state in different scenarios
     * 0. LOADING: start loading task
     * 1. FINISHED: successfully finish the loading
     * 2. ERROR: load failed
     * 3. SKIPPED: manually skip the file
     */
    public abstract void updateFileState(List<PipeFileRecord> files, PipeFileState state);

    /**
     * Cleanup expired file records
     */
    public abstract void cleanup();

    /**
     * Destroy the repo after dropping the pipe
     */
    public abstract void destroy();

    public enum PipeFileState {
        UNLOADED,
        LOADING,
        LOADED,
        SKIPPED,
        ERROR;
    }
}
