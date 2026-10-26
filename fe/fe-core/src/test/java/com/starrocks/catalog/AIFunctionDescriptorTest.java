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

import com.starrocks.builtins.VectorizedBuiltinFunctions;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AICapability;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AIFunctionDescriptor;
import com.starrocks.builtins.VectorizedBuiltinFunctions.AIPromptKind;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

class AIFunctionDescriptorTest {
    @Test
    void testPassthroughInputsExcludeSelectorsAndOptions() {
        // Prompt-only and text-only overloads, with and without a trailing options MAP.
        for (long id : List.of(200100L, 200101L, 200130L, 200131L)) {
            AIFunctionDescriptor descriptor = VectorizedBuiltinFunctions.getAIFunctionDescriptor(id);
            Assertions.assertEquals(AIPromptKind.PASSTHROUGH, descriptor.promptKind());
            Assertions.assertEquals(List.of(0), descriptor.inputArguments());
            Assertions.assertEquals(-1, descriptor.fileArgument());
        }
        // Explicit-model and named-provider overloads keep only their semantic input.
        for (long id : List.of(200102L, 200103L, 200132L, 200133L, 200140L, 200141L, 200142L, 200143L)) {
            AIFunctionDescriptor descriptor = VectorizedBuiltinFunctions.getAIFunctionDescriptor(id);
            Assertions.assertEquals(AIPromptKind.PASSTHROUGH, descriptor.promptKind());
            Assertions.assertEquals(List.of(1), descriptor.inputArguments());
            Assertions.assertEquals(-1, descriptor.fileArgument());
        }
    }

    @Test
    void testFileInputRolesPreserveTextPromptContracts() {
        for (long id : List.of(200104L, 200105L, 200106L, 200107L)) {
            AIFunctionDescriptor descriptor = VectorizedBuiltinFunctions.getAIFunctionDescriptor(id);
            int textArgument = id < 200106L ? 0 : 1;
            Assertions.assertEquals(AIPromptKind.PASSTHROUGH, descriptor.promptKind());
            Assertions.assertEquals(List.of(textArgument), descriptor.inputArguments());
            Assertions.assertEquals(textArgument + 1, descriptor.fileArgument());
        }
        for (long id : List.of(200134L, 200135L, 200136L, 200137L)) {
            AIFunctionDescriptor descriptor = VectorizedBuiltinFunctions.getAIFunctionDescriptor(id);
            Assertions.assertEquals(AIPromptKind.NONE, descriptor.promptKind());
            Assertions.assertEquals(List.of(), descriptor.inputArguments());
            Assertions.assertEquals(id < 200136L ? 0 : 1, descriptor.fileArgument());
        }
        Assertions.assertEquals(0, AIPromptKind.NONE.fixedBytes());
        Assertions.assertEquals(0, AIPromptKind.NONE.autoDetectFixedBytes());
    }

    @Test
    void testTemplateInputsPreserveTheirPromptContract() {
        AIFunctionDescriptor summarize = VectorizedBuiltinFunctions.getAIFunctionDescriptor(200124L);
        Assertions.assertEquals(AIPromptKind.SUMMARIZE, summarize.promptKind());
        Assertions.assertEquals(List.of(0), summarize.inputArguments());

        AIFunctionDescriptor classify = VectorizedBuiltinFunctions.getAIFunctionDescriptor(200113L);
        Assertions.assertEquals(AIPromptKind.CLASSIFY, classify.promptKind());
        Assertions.assertEquals(List.of(1, 2), classify.inputArguments());

        AIFunctionDescriptor translate = VectorizedBuiltinFunctions.getAIFunctionDescriptor(200121L);
        Assertions.assertEquals(AIPromptKind.TRANSLATE, translate.promptKind());
        Assertions.assertEquals(List.of(1, 2, 3), translate.inputArguments());
        Assertions.assertEquals(List.of(2), translate.nullAsEmptyArguments());
        Assertions.assertEquals(List.of(3), translate.blankAsNullArguments());
    }

    @Test
    void testInputArgumentsAreImmutableSnapshots() {
        List<Integer> inputs = new ArrayList<>(List.of(1));
        List<Integer> nullAsEmpty = new ArrayList<>(List.of(2));
        List<Integer> blankAsNull = new ArrayList<>(List.of(3));
        AIFunctionDescriptor descriptor = new AIFunctionDescriptor(
                AICapability.CHAT, 0, -1, -1, AIPromptKind.TRANSLATE, inputs, nullAsEmpty, blankAsNull);
        inputs.set(0, 0);
        nullAsEmpty.clear();
        blankAsNull.clear();

        Assertions.assertEquals(List.of(1), descriptor.inputArguments());
        Assertions.assertEquals(List.of(2), descriptor.nullAsEmptyArguments());
        Assertions.assertEquals(List.of(3), descriptor.blankAsNullArguments());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> descriptor.inputArguments().add(2));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> descriptor.nullAsEmptyArguments().clear());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> descriptor.blankAsNullArguments().clear());
        AIFunctionDescriptor generated = VectorizedBuiltinFunctions.getAIFunctionDescriptor(200100L);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> generated.inputArguments().set(0, 1));
    }
}
