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

package com.starrocks.load.pipe;

import com.google.common.collect.Lists;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.UserException;
import com.starrocks.fs.HdfsUtil;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.sql.ast.TableFunctionRelation;
import com.starrocks.thrift.TBrokerFileStatus;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

public class FilePipeSource extends PipeSource {

    private TableFunctionRelation dataSource;
    private BrokerFileGroup fileGroup;

    @Override
    List<PipePiece> pollPiece() throws UserException {
        // FIXME: get a broker desc
        BrokerDesc brokerDesc = null;
        List<TBrokerFileStatus> fileList = Lists.newArrayList();
        for (String path : fileGroup.getFilePaths()) {
            HdfsUtil.parseFile(path, brokerDesc, fileList);
        }
        List<PipePiece> pieces = Lists.newArrayList();
        FilePipePiece filePiece = new FilePipePiece();
        long sumSize = 0;
        for (TBrokerFileStatus file : fileList) {
            filePiece.getFile().add(file);
            sumSize += file.getSize();
            if (sumSize >= (1 << 30)) {
                pieces.add(filePiece);
                filePiece = new FilePipePiece();
                sumSize = 0;
            }
        }
        if (CollectionUtils.isNotEmpty(filePiece.getFile())) {
            pieces.add(filePiece);
        }
        return pieces;
    }

}
