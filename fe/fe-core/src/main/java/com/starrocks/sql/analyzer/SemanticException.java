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

package com.starrocks.sql.analyzer;

import com.starrocks.sql.common.ErrorType;
import com.starrocks.sql.common.StarRocksPlannerException;
import com.starrocks.sql.parser.NodePosition;
import org.apache.commons.lang3.StringUtils;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;
import static java.lang.String.format;

public class SemanticException extends StarRocksPlannerException {

    protected final String detailMsg;

    protected final NodePosition pos;

    protected boolean canAppend = true;


    public SemanticException(String formatString) {
        this(formatString, NodePosition.ZERO);
    }


    public SemanticException(String detailMsg, NodePosition pos) {
        super(detailMsg, ErrorType.USER_ERROR);
        this.detailMsg = detailMsg;
        this.pos = pos;
    }

    public SemanticException(String detailMsg, NodePosition pos, boolean canAppend) {
        super(detailMsg, ErrorType.USER_ERROR);
        this.detailMsg = detailMsg;
        this.pos = pos;
        this.canAppend = canAppend;
    }

    public SemanticException(String formatString, Object... args) {
        this(format(formatString, args), NodePosition.ZERO);
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

        if (StringUtils.isNotEmpty(detailMsg)) {
            builder.append(". Detail message: ");
            builder.append(detailMsg);
            builder.append(".");
        }
        return builder.toString();
    }

    public String getDetailMsg() {
        return detailMsg;
    }

    // append msg to previous exception msg, bug just only once.
    SemanticException appendOnlyOnceMsg(String appendMsg, NodePosition pos) {
        if (this.canAppend) {
            return new SemanticException(this.getDetailMsg() + " in " + appendMsg, pos, false);
        } else {
            return this;
        }
    }
}

