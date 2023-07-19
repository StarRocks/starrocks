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

import com.starrocks.persist.gson.GsonUtils;

import java.util.Objects;

public class PipeFile {

    public String path;
    public long size;
    public FileListRepo.PipeFileState state;

    public PipeFile(String path, long size, FileListRepo.PipeFileState state) {
        this.path = path;
        this.size = size;
        this.state = state;
    }

    public String getPath() {
        return path;
    }

    public long getSize() {
        return size;
    }

    public FileListRepo.PipeFileState getState() {
        return state;
    }

    public static PipeFile fromJson(String json) {
        return GsonUtils.GSON.fromJson(json, PipeFile.class);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PipeFile pipeFile = (PipeFile) o;
        return Objects.equals(path, pipeFile.path)
                && Objects.equals(state, pipeFile.state)
                && Objects.equals(size, pipeFile.size);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path);
    }

    @Override
    public String toString() {
        return "PipeFile{" +
                "path='" + path + '\'' +
                ", size=" + size +
                ", state=" + state +
                '}';
    }
}
