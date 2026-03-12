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

package com.starrocks.udf;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class UDFClassAnalyzerTest {

    // UDTF with generic List<String> parameters
    public static class GenericListUDTF {
        public String[] process(String input, List<String> tags, List<String> values) {
            return new String[]{input};
        }
    }

    // UDTF with raw List parameters
    @SuppressWarnings("rawtypes")
    public static class RawListUDTF {
        public String[] process(String input, List tags, List values) {
            return new String[]{input};
        }
    }

    @Test
    public void testGenericListSignatureContainsAngleBrackets() throws NoSuchMethodException {
        String signature = UDFClassAnalyzer.getSignature("process", GenericListUDTF.class);
        Assertions.assertNotNull(signature);
        Assertions.assertTrue(signature.contains("<"),
                "Generic List signature should contain '<' due to getGenericParameterTypes(): " + signature);
    }

    // Raw List params produce standard JNI signature without generics.
    @Test
    public void testRawListSignatureIsStandardJNI() throws NoSuchMethodException {
        String signature = UDFClassAnalyzer.getSignature("process", RawListUDTF.class);
        String expected = "(Ljava/lang/String;Ljava/util/List;Ljava/util/List;)[Ljava/lang/String;";
        Assertions.assertEquals(expected, signature);
    }
}
