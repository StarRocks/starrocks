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

package com.starrocks.sql.parser;

import org.apache.commons.lang3.StringUtils;

import static com.starrocks.sql.common.ErrorMsgProxy.PARSER_ERROR_MSG;
import static java.lang.String.format;

public class ParsingException extends RuntimeException {

    private final String detailMsg;

    private final NodePosition pos;

    public ParsingException(String detailMsg, NodePosition pos) {
        super(detailMsg);
        this.detailMsg = detailMsg;
        this.pos = pos;
    }

    // error message should contain position info. This method will be removed in the future.
    public ParsingException(String formatString, Object... args) {
        this(format(formatString, args), NodePosition.ZERO);
    }

    @Override
    public String getMessage() {
        StringBuilder builder = new StringBuilder("Getting syntax error");
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
}
