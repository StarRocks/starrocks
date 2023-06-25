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

import org.apache.commons.collections4.ListUtils;

import java.util.ArrayList;
import java.util.List;

public class FilePipePiece {

    private List<PipeFile> files;

    public FilePipePiece() {
        this.files = new ArrayList<>();
    }

    public FilePipePiece(List<PipeFile> files) {
        this.files = files;
    }

    public List<PipeFile> getFiles() {
        return files;
    }

    public void setFiles(List<PipeFile> files) {
        this.files = files;
    }

    public void addFiles(List<PipeFile> files) {
        this.files.addAll(files);
    }

    public void addFile(PipeFile file) {
        this.files.add(file);
    }

    public long getTotalBytes() {
        return ListUtils.emptyIfNull(files).stream().map(PipeFile::getSize).reduce(0L, Long::sum);
    }

    public long getTotalRows() {
        // FIXME: implement it
        return 1;
    }

}
