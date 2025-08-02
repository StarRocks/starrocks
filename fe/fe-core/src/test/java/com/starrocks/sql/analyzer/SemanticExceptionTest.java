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

import com.starrocks.sql.parser.NodePosition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SemanticExceptionTest {
    @Test
    public void semanticExceptionIncludesDetailMessage() {
        SemanticException exception = new SemanticException("Error occurred");
        Assertions.assertEquals("Error occurred", exception.getDetailMsg());
    }

    @Test
    public void semanticExceptionFormatsMessageWithPosition() {
        NodePosition position = new NodePosition(1, 5, 1, 10);
        SemanticException exception = new SemanticException("Error occurred", position);
        String message = exception.getMessage();
        Assertions.assertTrue(message.contains("Getting analyzing error"));
        Assertions.assertTrue(message.contains("line 1, column 5 to line 1, column 10"));
        Assertions.assertTrue(message.contains("Detail message: Error occurred."));
    }

    @Test
    public void semanticExceptionAppendsMessageOnlyOnce() {
        NodePosition position = new NodePosition(1, 1, 1, 1);
        SemanticException exception = new SemanticException("Initial error", position);
        SemanticException appendedException = exception.appendOnlyOnceMsg("additional context", position);
        Assertions.assertEquals("Initial error in additional context", appendedException.getDetailMsg());
        SemanticException secondAppend = appendedException.appendOnlyOnceMsg("another context", position);
        Assertions.assertEquals("Initial error in additional context", secondAppend.getDetailMsg());
    }

    @Test
    public void semanticExceptionIncludesCauseInMessage() {
        Exception cause = new RuntimeException("Root cause");
        SemanticException exception = new SemanticException("Error occurred", cause);
        String message = exception.getMessage();
        Assertions.assertTrue(message.contains("Getting analyzing error"));
        Assertions.assertTrue(message.contains("Detail message: Error occurred."));
        Assertions.assertTrue(message.contains("Cause: java.lang.RuntimeException: Root cause"));
    }
}
