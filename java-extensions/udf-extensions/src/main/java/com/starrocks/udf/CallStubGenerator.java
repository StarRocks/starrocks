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

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

import static org.objectweb.asm.Opcodes.AALOAD;
import static org.objectweb.asm.Opcodes.AASTORE;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ANEWARRAY;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.ASTORE;
import static org.objectweb.asm.Opcodes.F_APPEND;
import static org.objectweb.asm.Opcodes.F_CHOP;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.IF_ICMPGE;
import static org.objectweb.asm.Opcodes.ILOAD;
import static org.objectweb.asm.Opcodes.INTEGER;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.ISTORE;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.V1_8;

public class CallStubGenerator {
    // generate call stub name
    public static final String CLAZZ_NAME = "com/starrocks/udf/gen/CallStub";
    public static final String GEN_KEYWORD = "com.starrocks.udf.gen";

    // generate batch update
    // public class CallStub {
    //     public static void batchCallV(int rows, UDAFSum obj, State var0, Integer[] var1, ...) throws Exception {
    //         for(int i = 0; i < rows; ++i) {
    //             obj.update(var0, var1[i], ...);
    //         }
    //     }
    // }
    private static class AggBatchCallGenerator {
        AggBatchCallGenerator(Class<?> clazz, Method update) {
            this.udafClazz = clazz;
            this.udafUpdate = update;
        }

        private final Class<?> udafClazz;
        private final Method udafUpdate;

        private final ClassWriter writer = new ClassWriter(0);

        private void declareCallStubClazz() {
            writer.visit(V1_8, ACC_PUBLIC, CLAZZ_NAME, null, "java/lang/Object", null);
        }

        // int numRows, FunctionCallClz obj, FunctionCall.State state, Integer[] a
        private void genBatchUpdateSingle() {
            final Parameter[] parameters = udafUpdate.getParameters();
            StringBuilder desc = new StringBuilder("(");
            desc.append("I");
            desc.append(Type.getDescriptor(udafClazz));
            for (int i = 0; i < parameters.length; i++) {
                final Class<?> type = parameters[i].getType();
                if (type.isPrimitive()) {
                    throw new UnsupportedOperationException("Unsupported Primitive Type:" + type.getTypeName());
                }
                if (i > 0) {
                    desc.append("[");
                }
                desc.append(Type.getDescriptor(type));
            }

            final Class<?> returnType = udafUpdate.getReturnType();
            if (returnType != void.class) {
                throw new UnsupportedOperationException("Unsupported return Type:" + returnType.getTypeName());
            }
            desc.append(")V");

            final MethodVisitor batchCall =
                    writer.visitMethod(ACC_PUBLIC + ACC_STATIC, "batchCallV", desc.toString(), null,
                            new String[] {"java/lang/Exception"});

            batchCall.visitCode();

            final Label l0 = new Label();
            batchCall.visitLabel(l0);
            // for (int i = 0...)
            batchCall.visitInsn(ICONST_0);
            // load local i
            int iIdx = 2 + parameters.length;
            batchCall.visitVarInsn(ISTORE, iIdx);

            final Label l1 = new Label();
            batchCall.visitLabel(l1);
            batchCall.visitFrame(F_APPEND, 1, new Object[] {INTEGER}, 0, null);

            batchCall.visitVarInsn(ILOAD, iIdx);
            batchCall.visitVarInsn(ILOAD, 0);

            final Label l2 = new Label();
            batchCall.visitJumpInsn(IF_ICMPGE, l2);

            final Label l3 = new Label();
            batchCall.visitLabel(l3);
            batchCall.visitVarInsn(ALOAD, 1);
            batchCall.visitVarInsn(ALOAD, 2);

            for (int i = 3; i < 3 + parameters.length - 1; i++) {
                batchCall.visitVarInsn(ALOAD, i);
                batchCall.visitVarInsn(ILOAD, iIdx);
                batchCall.visitInsn(AALOAD);
            }

            batchCall.visitMethodInsn(INVOKEVIRTUAL, Type.getInternalName(udafClazz), udafUpdate.getName(),
                    Type.getMethodDescriptor(udafUpdate), false);
            final Label l4 = new Label();
            batchCall.visitLabel(l4);
            batchCall.visitIincInsn(iIdx, 1);
            batchCall.visitJumpInsn(GOTO, l1);

            batchCall.visitLabel(l2);
            // pop frame and return
            batchCall.visitFrame(F_CHOP, 1, new Object[] {INTEGER}, 0, null);
            batchCall.visitInsn(RETURN);

            // define local variables
            final Label l5 = new Label();
            batchCall.visitLabel(l5);
            int callStubInputParameters = 3 + parameters.length;
            batchCall.visitMaxs(callStubInputParameters, callStubInputParameters + 1);
            batchCall.visitEnd();
        }

        private void finish() {
            writer.visitEnd();
        }

        private byte[] getByteCode() {
            return writer.toByteArray();
        }
    }

    public static byte[] generateCallStubV(Class<?> clazz, Method method) {
        final AggBatchCallGenerator generator = new AggBatchCallGenerator(clazz, method);
        generator.declareCallStubClazz();
        generator.genBatchUpdateSingle();
        generator.finish();
        return generator.getByteCode();
    }

    //    public class CallStub {
    //        public static void batchCallV(int rows, UDF obj, TYPE[] var1, Integer[] var2) throws Exception {
    //            for(int var = 0; var < rows; ++var) {
    //                obj.update(var0, var1[var8], var2[var8]);
    //            }
    //        }
    //    }
    private static class BatchCallEvaluateGenerator {
        BatchCallEvaluateGenerator(Class<?> clazz, Method update) {
            this.udfClazz = clazz;
            this.udfEvaluate = update;
        }

        private final ClassWriter writer = new ClassWriter(0);

        private void declareCallStubClazz() {
            writer.visit(V1_8, ACC_PUBLIC, CLAZZ_NAME, null, "java/lang/Object", null);
        }

        private void genBatchUpdateSingle() {
            final Parameter[] parameters = udfEvaluate.getParameters();
            StringBuilder desc = new StringBuilder("(");
            desc.append("I");
            desc.append(Type.getDescriptor(udfClazz));
            for (Parameter parameter : parameters) {
                final Class<?> type = parameter.getType();
                if (type.isPrimitive()) {
                    throw new UnsupportedOperationException("Unsupported Primitive Type:" + type.getTypeName());
                }
                desc.append("[");
                desc.append(Type.getDescriptor(type));
            }

            final Class<?> returnType = udfEvaluate.getReturnType();
            if (returnType.isPrimitive()) {
                throw new UnsupportedOperationException("Unsupported return Type:" + returnType.getTypeName());
            }
            desc.append(")");
            desc.append("[").append(Type.getDescriptor(returnType));

            final MethodVisitor batchCall =
                    writer.visitMethod(ACC_PUBLIC + ACC_STATIC, "batchCallV", desc.toString(), null,
                            new String[] {"java/lang/Exception"});

            batchCall.visitCode();

            // local var1: rows
            // local var2: UDAF handle
            // local var3...varn: left paramters

            // RET_TYPE[] res;
            int resIndex = 2 + parameters.length;
            // int i;
            int iIndex = resIndex + 1;

            final Label l0 = new Label();
            batchCall.visitLabel(l0);
            // RETURN_TYPE[] = new RETURN_TYPE[num_rows]
            batchCall.visitVarInsn(ILOAD, 0);
            batchCall.visitTypeInsn(ANEWARRAY, Type.getInternalName(returnType));
            batchCall.visitVarInsn(ASTORE, resIndex);

            // PUSH FRAME
            final Label l1 = new Label();
            batchCall.visitLabel(l1);
            batchCall.visitInsn(ICONST_0);
            batchCall.visitVarInsn(ISTORE, iIndex);

            final Label l2 = new Label();
            batchCall.visitLabel(l2);
            batchCall.visitFrame(F_APPEND, 2, new Object[] {"[" + Type.getDescriptor(returnType), INTEGER}, 0, null);
            batchCall.visitVarInsn(ILOAD, iIndex);
            batchCall.visitVarInsn(ILOAD, 0);

            final Label l3 = new Label();
            batchCall.visitJumpInsn(IF_ICMPGE, l3);

            final Label l4 = new Label();
            batchCall.visitLabel(l4);
            // load res[i]
            batchCall.visitVarInsn(ALOAD, resIndex);
            batchCall.visitVarInsn(ILOAD, iIndex);
            // load obj
            batchCall.visitVarInsn(ALOAD, 1);
            int padding = 2;
            for (int i = 0; i < parameters.length; i++) {
                batchCall.visitVarInsn(ALOAD, i + padding);
                batchCall.visitVarInsn(ILOAD, iIndex);
                batchCall.visitInsn(AALOAD);
            }

            batchCall.visitMethodInsn(INVOKEVIRTUAL, Type.getInternalName(udfClazz), udfEvaluate.getName(),
                    Type.getMethodDescriptor(udfEvaluate), false);
            batchCall.visitInsn(AASTORE);

            final Label l5 = new Label();
            batchCall.visitLabel(l5);
            batchCall.visitIincInsn(iIndex, 1);
            batchCall.visitJumpInsn(GOTO, l2);

            batchCall.visitLabel(l3);
            batchCall.visitFrame(F_CHOP, 1, new Object[] {INTEGER}, 0, null);
            batchCall.visitVarInsn(ALOAD, resIndex);
            batchCall.visitInsn(ARETURN);

            final Label l6 = new Label();
            batchCall.visitLabel(l6);
            batchCall.visitLocalVariable("i", "I", null, l2, l3, iIndex);
            batchCall.visitLocalVariable("res", Type.getDescriptor(returnType), null, l1, l6, resIndex);
            batchCall.visitMaxs(iIndex + 1, iIndex + 1);
            batchCall.visitEnd();
        }

        private void finish() {
            writer.visitEnd();
        }

        private byte[] getByteCode() {
            return writer.toByteArray();
        }

        private final Class<?> udfClazz;
        private final Method udfEvaluate;
    }

    public static byte[] generateScalarCallStub(Class<?> clazz, Method method) {
        final BatchCallEvaluateGenerator generator = new BatchCallEvaluateGenerator(clazz, method);
        generator.declareCallStubClazz();
        generator.genBatchUpdateSingle();
        generator.finish();
        return generator.getByteCode();
    }

}
