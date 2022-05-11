// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

package com.starrocks.udf;

import jdk.internal.org.objectweb.asm.ClassWriter;
import jdk.internal.org.objectweb.asm.Label;
import jdk.internal.org.objectweb.asm.MethodVisitor;
import jdk.internal.org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

import static jdk.internal.org.objectweb.asm.Opcodes.*;

public class CallStubGenerator {
    // generate call stub name
    public static final String CLAZZ_NAME = "com/starrocks/udf/gen/CallStub";
    public static final String GEN_KEYWORD = "com.starrocks.udf.gen";

    //  generate batch update
    //  for(int i = 0; i < rows; ++i) {
    //    obj.update(var2, ...);
    //  }
    private static class AggBatchCallGenerator {
        AggBatchCallGenerator(Class<?> clazz, Method update) {
            this.UDAFClazz = clazz;
            this.UDAFUpdate = update;
        }

        private final Class<?> UDAFClazz;
        private final Method UDAFUpdate;

        private final ClassWriter writer = new ClassWriter(0);

        void declareCallStubClazz() {
            writer.visit(V1_8, ACC_PUBLIC, CLAZZ_NAME, null, "java/lang/Object", null);
        }

        // int numRows, FunctionCallClz obj, FunctionCall.State state, Integer[] a
        void genBatchUpdateSingle() {
            final Parameter[] parameters = UDAFUpdate.getParameters();
            StringBuilder desc = new StringBuilder("(");
            desc.append("I");
            desc.append(Type.getDescriptor(UDAFClazz));
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

            final Class<?> returnType = UDAFUpdate.getReturnType();
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
            int i_idx = 2 + parameters.length;
            batchCall.visitVarInsn(ISTORE, i_idx);

            final Label l1 = new Label();
            batchCall.visitLabel(l1);
            batchCall.visitFrame(F_APPEND, 1, new Object[] {INTEGER}, 0, null);

            batchCall.visitVarInsn(ILOAD, i_idx);
            batchCall.visitVarInsn(ILOAD, 0);

            final Label l2 = new Label();
            batchCall.visitJumpInsn(IF_ICMPGE, l2);

            final Label l3 = new Label();
            batchCall.visitLabel(l3);
            batchCall.visitVarInsn(ALOAD, 1);
            batchCall.visitVarInsn(ALOAD, 2);

            for (int i = 3; i < 3 + parameters.length - 1; i++) {
                batchCall.visitVarInsn(ALOAD, i);
                batchCall.visitVarInsn(ILOAD, i_idx);
                batchCall.visitInsn(AALOAD);
            }

            batchCall.visitMethodInsn(INVOKEVIRTUAL, Type.getInternalName(UDAFClazz), UDAFUpdate.getName(),
                    Type.getMethodDescriptor(UDAFUpdate), false);
            final Label l4 = new Label();
            batchCall.visitLabel(l4);
            batchCall.visitIincInsn(i_idx, 1);
            batchCall.visitJumpInsn(GOTO, l1);

            batchCall.visitLabel(l2);
            // pop frame and return
            batchCall.visitFrame(F_CHOP, 1, new Object[] {INTEGER}, 0, null);
            batchCall.visitInsn(RETURN);

            // define local variables
            final Label l5 = new Label();
            batchCall.visitLabel(l5);
            batchCall.visitLocalVariable("i", "I", null, l1, l2, i_idx);
            batchCall.visitLocalVariable("rows", "I", null, l1, l5, 0);
            batchCall.visitLocalVariable("obj", "L" + Type.getInternalName(UDAFClazz) + ";", null, l0, l5, 1);
            int padding = 2;
            for (int i = 0; i < parameters.length; ++i) {
                final Class<?> type = parameters[i].getType();
                batchCall.visitLocalVariable("var" + i, "L" + Type.getInternalName(type) + ";", null, l1, l5,
                        i + padding);
            }
            int callStubInputParameters = 3 + parameters.length;
            batchCall.visitMaxs(callStubInputParameters, callStubInputParameters + 1);
            batchCall.visitEnd();
        }

        void finish() {
            writer.visitEnd();
        }

        byte[] getByteCode() {
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
}
