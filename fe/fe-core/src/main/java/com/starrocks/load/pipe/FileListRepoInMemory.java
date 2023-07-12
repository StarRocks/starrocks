// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.load.pipe;

import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileListRepoInMemory {

    private static final Logger LOG = LogManager.getLogger(FileListRepoInMemory.class);

    // TODO: persist the file list
    private final Map<String, PipeFile> fileList = new HashMap<>();

    public List<PipeFile> listFiles() {
        return new ArrayList<>(fileList.values());
    }

    public List<PipeFile> getUnloadedFiles() {
        return fileList.values().stream().filter(x -> x.state.equals(FileListRepo.PipeFileState.UNLOADED))
                .collect(Collectors.toList());
    }

    public int size() {
        return fileList.size();
    }

    public void addBrokerFiles(List<TBrokerFileStatus> files) {
        for (TBrokerFileStatus file : files) {
            PipeFile pfile = new PipeFile(file.getPath(), file.getSize(), FileListRepo.PipeFileState.UNLOADED);
            fileList.put(file.getPath(), pfile);
        }
        LOG.debug("add broker-files into repo: " + files);
    }

    public void addFiles(List<PipeFile> files) {
        for (PipeFile file : files) {
            fileList.put(file.path, file);
        }
    }

    public void updateFiles(List<PipeFile> files, FileListRepo.PipeFileState state) {
        // TODO: optimize the performance
        for (PipeFile file : files) {
            PipeFile existed = fileList.get(file.path);
            if (existed != null) {
                existed.state = state;
            } else {
                fileList.put(file.path, file);
            }
        }
        LOG.debug("update file state to {}: {}", state, files);
    }

    public void gcFiles() {
        // TODO: GC file list
    }

}
