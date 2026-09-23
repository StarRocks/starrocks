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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.sql.analyzer.SemanticException;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RewriteToVectorPlanRuleTest {

    /** The parse appends into the sink the traversal accumulates into; collect it for assertions. */
    private static float[] parse(String literal) {
        List<Float> out = new ArrayList<>();
        RewriteToVectorPlanRule.parseStringAsFloatArray(literal, out);
        float[] vector = new float[out.size()];
        for (int i = 0; i < vector.length; i++) {
            vector[i] = out.get(i);
        }
        return vector;
    }

    @Test
    public void testOrdinaryDecimals() {
        assertArrayEquals(new float[] {0.0f, 1.0f, -1.0f, 0.5f, -0.25f, 1024.0f, -3.75f},
                parse("[0,1,-1,0.5,-0.25,1024,-3.75]"), 0.0f);
    }

    @Test
    public void testExponentAndSignForms() {
        assertArrayEquals(new float[] {100.0f, 2.5f, -15.0f, 0.0f}, parse("[1e2,+2.5,-1.5E1,0.0]"), 0.0f);
    }

    @Test
    public void testWhitespaceAroundElements() {
        assertArrayEquals(new float[] {1.0f, 2.0f, 3.0f}, parse("[ 1.0 ,\t2.0 ,3.0 ]"), 0.0f);
    }

    @Test
    public void testEmptyLiteralYieldsNoElements() {
        assertEquals(0, parse("[]").length);
        assertEquals(0, parse("[  ]").length);
    }

    @Test
    public void testMatchesJdkBitForBit() {
        // Narrowing the correctly-rounded double is not a second rounding error: binary64 has 53
        // significant bits and safe double rounding into binary32 needs 2*24+2 = 50. Pin that, over
        // both the shortest float32 repr and the 17-digit float64 repr a client may send.
        Random rnd = new Random(20260921L);
        StringBuilder sb = new StringBuilder("[");
        String[] tokens = new String[4096];
        for (int i = 0; i < tokens.length; i++) {
            float f = (float) (rnd.nextGaussian() * Math.pow(10, rnd.nextInt(12) - 6));
            tokens[i] = (i % 2 == 0) ? Float.toString(f) : Double.toString(f);
            if (i > 0) {
                sb.append(',');
            }
            sb.append(tokens[i]);
        }
        float[] got = parse(sb.append(']').toString());
        assertEquals(tokens.length, got.length);
        for (int i = 0; i < tokens.length; i++) {
            assertEquals(Float.floatToRawIntBits(Float.parseFloat(tokens[i])), Float.floatToRawIntBits(got[i]),
                    "token '" + tokens[i] + "'");
        }
    }

    @Test
    public void testSubnormalAndExtremes() {
        for (String token : new String[] {Float.toString(Float.MIN_VALUE), Float.toString(Float.MAX_VALUE),
                Float.toString(Math.nextUp(1.0f)), "1.4e-45", "3.4028235e38"}) {
            assertEquals(Float.floatToRawIntBits(Float.parseFloat(token)),
                    Float.floatToRawIntBits(parse("[" + token + "]")[0]), token);
        }
    }

    @Test
    public void testRejectsMalformedLiteral() {
        assertThrows(SemanticException.class, () -> parse(null));
        assertThrows(SemanticException.class, () -> parse("1.0,2.0"));
        assertThrows(SemanticException.class, () -> parse("[1.0,]"));
        assertThrows(SemanticException.class, () -> parse("[1.0,,2.0]"));
        assertThrows(SemanticException.class, () -> parse("[abc]"));
        assertThrows(SemanticException.class, () -> parse("[1.2.3]"));
    }

    @Test
    public void testRejectsNonFiniteAndFloatOverflow() {
        assertThrows(SemanticException.class, () -> parse("[NaN]"));
        assertThrows(SemanticException.class, () -> parse("[Infinity]"));
        assertThrows(SemanticException.class, () -> parse("[1e400]"));
        // Finite as a double but past FLT_MAX. The check used to run on the double, so this reached
        // the BE as a silent +inf.
        assertThrows(SemanticException.class, () -> parse("[3.5e38]"));
        assertThrows(SemanticException.class, () -> parse("[-3.5e38]"));
    }

    @Test
    public void testUnderflowToZeroIsAccepted() {
        // Too small for float32 is not an error anywhere in the stack; it becomes signed zero.
        assertArrayEquals(new float[] {0.0f, -0.0f}, parse("[1e-300,-1e-300]"), 0.0f);
    }
}
