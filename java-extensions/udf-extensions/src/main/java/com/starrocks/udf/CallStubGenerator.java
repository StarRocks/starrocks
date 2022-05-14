// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.udf;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Type;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;

import static org.objectweb.asm.Opcodes.AALOAD;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_STATIC;
import static org.objectweb.asm.Opcodes.ALOAD;
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
            batchCall.visitLocalVariable("i", "I", null, l1, l2, iIdx);
            batchCall.visitLocalVariable("rows", "I", null, l1, l5, 0);
            batchCall.visitLocalVariable("obj", "L" + Type.getInternalName(udafClazz) + ";", null, l0, l5, 1);
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
}
