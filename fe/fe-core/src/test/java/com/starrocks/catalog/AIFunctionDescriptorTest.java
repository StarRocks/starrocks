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
        }
        // Explicit-model and named-provider overloads keep only their semantic input.
        for (long id : List.of(200102L, 200103L, 200132L, 200133L, 200140L, 200141L, 200142L, 200143L)) {
            AIFunctionDescriptor descriptor = VectorizedBuiltinFunctions.getAIFunctionDescriptor(id);
            Assertions.assertEquals(AIPromptKind.PASSTHROUGH, descriptor.promptKind());
            Assertions.assertEquals(List.of(1), descriptor.inputArguments());
        }
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
    }

    @Test
    void testInputArgumentsAreImmutableSnapshots() {
        List<Integer> inputs = new ArrayList<>(List.of(1));
        AIFunctionDescriptor descriptor = new AIFunctionDescriptor(
                AICapability.CHAT, 0, -1, AIPromptKind.PASSTHROUGH, inputs);
        inputs.set(0, 0);

        Assertions.assertEquals(List.of(1), descriptor.inputArguments());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> descriptor.inputArguments().add(2));
        AIFunctionDescriptor generated = VectorizedBuiltinFunctions.getAIFunctionDescriptor(200100L);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> generated.inputArguments().set(0, 1));
    }
}
