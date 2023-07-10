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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.parser.NodePosition;
import org.apache.commons.lang.StringUtils;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;

public class UnsupportedMVException extends SemanticException {

    public UnsupportedMVException(String formatString) {
        super(formatString);
    }

    public UnsupportedMVException(String detailMsg, NodePosition pos) {
        super(detailMsg, pos);
    }

    public UnsupportedMVException(String formatString, Object... args) {
        super(formatString, args);
    }

    @Override
    public String getMessage() {
        StringBuilder builder = new StringBuilder("Getting analyzing error");
        if (pos == null || pos.isZero()) {
            // no position info. do nothing.
        } else if (pos.getLine() == pos.getEndLine() && pos.getCol() == pos.getEndCol()) {
            builder.append(" ");
            builder.append(PARSER_ERROR_MSG.nodePositionPoint(pos.getLine(), pos.getCol()));
        } else {
            builder.append(" ");
            builder.append(PARSER_ERROR_MSG.nodePositionRange(pos.getLine(), pos.getCol(),
                    pos.getEndLine(), pos.getEndCol()));
        }

        if (StringUtils.isNotEmpty(getDetailMsg())) {
            builder.append(". Detail message: ");
            builder.append(detailMsg);
            builder.append(". Please use Asynchronous Materialized View instead");
        }

        return builder.toString();
    }
}
