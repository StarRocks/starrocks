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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.RecordComponent;

import static org.objectweb.asm.Opcodes.AALOAD;
import static org.objectweb.asm.Opcodes.AASTORE;
import static org.objectweb.asm.Opcodes.ACC_FINAL;
import static org.objectweb.asm.Opcodes.ACC_PUBLIC;
import static org.objectweb.asm.Opcodes.ACC_SUPER;
import static org.objectweb.asm.Opcodes.ACC_SYNTHETIC;
import static org.objectweb.asm.Opcodes.ALOAD;
import static org.objectweb.asm.Opcodes.ANEWARRAY;
import static org.objectweb.asm.Opcodes.ARETURN;
import static org.objectweb.asm.Opcodes.ASTORE;
import static org.objectweb.asm.Opcodes.BALOAD;
import static org.objectweb.asm.Opcodes.BIPUSH;
import static org.objectweb.asm.Opcodes.CHECKCAST;
import static org.objectweb.asm.Opcodes.DUP;
import static org.objectweb.asm.Opcodes.GOTO;
import static org.objectweb.asm.Opcodes.ICONST_0;
import static org.objectweb.asm.Opcodes.IFNE;
import static org.objectweb.asm.Opcodes.IFNULL;
import static org.objectweb.asm.Opcodes.IF_ICMPGE;
import static org.objectweb.asm.Opcodes.ILOAD;
import static org.objectweb.asm.Opcodes.INVOKESPECIAL;
import static org.objectweb.asm.Opcodes.INVOKESTATIC;
import static org.objectweb.asm.Opcodes.INVOKEVIRTUAL;
import static org.objectweb.asm.Opcodes.ISTORE;
import static org.objectweb.asm.Opcodes.NEW;
import static org.objectweb.asm.Opcodes.RETURN;
import static org.objectweb.asm.Opcodes.SIPUSH;
import static org.objectweb.asm.Opcodes.V11;

/**
 * Converts between Java records and StarRocks STRUCT columns, one codec per record class.
 *
 * <p>A STRUCT crosses the Java UDF boundary as one {@code Object[]} per field ("per-field arrays"):
 * <ul>
 *   <li>{@link #drain}: UDF/UDAF result records -> per-field arrays (STRUCT output);</li>
 *   <li>{@link #build}: per-field arrays -> records handed to the UDF/UDAF (STRUCT input).</li>
 * </ul>
 *
 * <p>Both directions run once per row per field, so reflection here dominates the cost of small
 * STRUCT-returning UDFs. Like {@link CallStubGenerator} does for evaluate/update, the codec generates
 * a class per record with direct {@code invokevirtual} accessor calls and a direct
 * {@code new Record(...)}, which the JIT can inline.
 *
 * <p>The generated class is defined in the record's own class loader and package (via
 * {@link MethodHandles.Lookup#defineClass} on a {@link MethodHandles#privateLookupIn} lookup), so it
 * can reach package-private records and is unloaded together with the UDF class loader. A hidden
 * nestmate class is not an option: a UDF record lives in UDFClassLoader's unnamed module, and a
 * cross-module private lookup lacks the full-privilege access defineHiddenClass requires.
 *
 * <p>Records whose canonical constructor is private (e.g. private nested records) or for which
 * generation fails for any other reason use reflection instead, with accessors and the canonical
 * constructor resolved once per record class rather than per batch.
 */
public final class RecordCodec {
    /** record[] -> per-field arrays. Rows whose record is null are skipped (left null). */
    public interface Drainer {
        void drain(int numRows, Object[] records, Object[][] perField);
    }

    /** per-field arrays -> record[]. Rows with {@code nulls[r] != 0} are left null; {@code nulls} may be null. */
    public interface Builder {
        Object[] build(int numRows, byte[] nulls, Object[] fieldArrays);
    }

    private static final ClassValue<RecordCodec> CODECS = new ClassValue<RecordCodec>() {
        @Override
        protected RecordCodec computeValue(Class<?> recordClass) {
            return create(recordClass);
        }
    };

    private final Class<?> recordClass;
    private final int numComponents;
    private final Drainer drainer;
    private final Builder builder;
    private final boolean generated;

    private RecordCodec(Class<?> recordClass, int numComponents, Drainer drainer, Builder builder,
                        boolean generated) {
        this.recordClass = recordClass;
        this.numComponents = numComponents;
        this.drainer = drainer;
        this.builder = builder;
        this.generated = generated;
    }

    public static RecordCodec of(Class<?> recordClass) {
        return CODECS.get(recordClass);
    }

    public int numComponents() {
        return numComponents;
    }

    /** Whether this codec uses generated bytecode (false: reflection fallback). */
    public boolean isGenerated() {
        return generated;
    }

    public void drain(int numRows, Object[] records, Object[][] perField) {
        if (perField.length != numComponents) {
            throw new IllegalStateException("Record " + recordClass.getName() + " has " + numComponents
                    + " components but STRUCT type has " + perField.length + " fields");
        }
        try {
            drainer.drain(numRows, records, perField);
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to access record components on " + recordClass.getName(), e);
        }
    }

    public Object[] build(int numRows, byte[] nulls, Object[] fieldArrays) {
        if (fieldArrays.length != numComponents) {
            throw new IllegalStateException("Record " + recordClass.getName() + " has " + numComponents
                    + " components but STRUCT type has " + fieldArrays.length + " fields");
        }
        try {
            return builder.build(numRows, nulls, fieldArrays);
        } catch (RuntimeException e) {
            throw new RuntimeException("Failed to construct record " + recordClass.getName(), e);
        }
    }

    // Synchronized so that concurrent first uses of a record class do not race to define the same
    // codec class (the loser would get a duplicate-definition LinkageError and fall back to reflection).
    private static synchronized RecordCodec create(Class<?> recordClass) {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class " + recordClass + " is not a record");
        }
        RecordComponent[] comps = recordClass.getRecordComponents();
        try {
            Object impl = generate(recordClass, comps);
            if (impl != null) {
                return new RecordCodec(recordClass, comps.length, (Drainer) impl, (Builder) impl, true);
            }
        } catch (Exception | LinkageError e) {
            // fall through to reflection
        }
        return reflective(recordClass);
    }

    // Visible for testing: the reflection fallback.
    static RecordCodec reflective(Class<?> recordClass) {
        if (!recordClass.isRecord()) {
            throw new IllegalArgumentException("Class " + recordClass + " is not a record");
        }
        RecordComponent[] comps = recordClass.getRecordComponents();
        int numFields = comps.length;
        Method[] accessors = new Method[numFields];
        Class<?>[] paramTypes = new Class<?>[numFields];
        for (int f = 0; f < numFields; f++) {
            accessors[f] = comps[f].getAccessor();
            accessors[f].setAccessible(true);
            paramTypes[f] = comps[f].getType();
        }
        Constructor<?> ctor;
        try {
            ctor = recordClass.getDeclaredConstructor(paramTypes);
            ctor.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Canonical constructor not found for record " + recordClass, e);
        }

        Drainer drainer = (numRows, records, perField) -> {
            try {
                for (int r = 0; r < numRows; r++) {
                    Object rec = records[r];
                    if (rec == null) {
                        continue;
                    }
                    for (int f = 0; f < numFields; f++) {
                        perField[f][r] = accessors[f].invoke(rec);
                    }
                }
            } catch (InvocationTargetException e) {
                throw rethrow(e.getTargetException());
            } catch (IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
        };
        Builder builder = (numRows, nulls, fieldArrays) -> {
            Object[] out = (Object[]) Array.newInstance(recordClass, numRows);
            Object[] args = new Object[numFields];
            try {
                for (int r = 0; r < numRows; r++) {
                    if (nulls != null && nulls[r] != 0) {
                        continue;
                    }
                    for (int f = 0; f < numFields; f++) {
                        args[f] = ((Object[]) fieldArrays[f])[r];
                    }
                    out[r] = ctor.newInstance(args);
                }
            } catch (InvocationTargetException e) {
                throw rethrow(e.getTargetException());
            } catch (InstantiationException | IllegalAccessException e) {
                throw new IllegalStateException(e);
            }
            return out;
        };
        return new RecordCodec(recordClass, numFields, drainer, builder, false);
    }

    private static RuntimeException rethrow(Throwable t) {
        if (t instanceof RuntimeException) {
            return (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        return new RuntimeException(t);
    }

    // ------------------------------------------------------------------ codegen

    static final String CODEC_SUFFIX = "$$StarRocksRecordCodec";

    /** Returns a Drainer & Builder instance, or null if the record cannot be handled by generated code. */
    private static Object generate(Class<?> recordClass, RecordComponent[] comps)
            throws ReflectiveOperationException {
        Class<?>[] paramTypes = new Class<?>[comps.length];
        for (int f = 0; f < comps.length; f++) {
            paramTypes[f] = comps[f].getType();
        }
        // The codec is a separate (non-nestmate) class in the record's package: it can call
        // package-private constructors but not private ones.
        if (Modifier.isPrivate(recordClass.getDeclaredConstructor(paramTypes).getModifiers())) {
            return null;
        }
        String codecBinaryName = recordClass.getName() + CODEC_SUFFIX;
        ClassLoader loader = recordClass.getClassLoader();
        Class<?> codecClass;
        try {
            // Already defined for this record (e.g. by an earlier codec whose ClassValue entry was cleared).
            codecClass = Class.forName(codecBinaryName, false, loader);
        } catch (ClassNotFoundException e) {
            MethodHandles.Lookup recordLookup = MethodHandles.privateLookupIn(recordClass, MethodHandles.lookup());
            byte[] bytes = generateBytes(codecBinaryName.replace('.', '/'), recordClass, comps);
            codecClass = recordLookup.defineClass(bytes);
        }
        return codecClass.getDeclaredConstructor().newInstance();
    }

    // Visible for testing.
    static byte[] generateBytes(String codecName, Class<?> recordClass, RecordComponent[] comps) {
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS) {
            @Override
            protected String getCommonSuperClass(String type1, String type2) {
                // Frames only merge at the loop head, where every reference local is re-loaded and
                // re-cast before use; avoid loading user classes from ASM's class loader.
                return "java/lang/Object";
            }
        };
        cw.visit(V11, ACC_PUBLIC | ACC_FINAL | ACC_SUPER | ACC_SYNTHETIC, codecName, null, "java/lang/Object",
                new String[] {Type.getInternalName(Drainer.class), Type.getInternalName(Builder.class)});

        MethodVisitor init = cw.visitMethod(ACC_PUBLIC, "<init>", "()V", null, null);
        init.visitCode();
        init.visitVarInsn(ALOAD, 0);
        init.visitMethodInsn(INVOKESPECIAL, "java/lang/Object", "<init>", "()V", false);
        init.visitInsn(RETURN);
        init.visitMaxs(0, 0);
        init.visitEnd();

        String recordName = Type.getInternalName(recordClass);
        genDrain(cw, recordName, comps);
        genBuild(cw, recordName, comps);
        cw.visitEnd();
        return cw.toByteArray();
    }

    // public void drain(int numRows, Object[] records, Object[][] perField) {
    //     for (int r = 0; r < numRows; r++) {
    //         Object rec = records[r];
    //         if (rec == null) continue;
    //         perField[0][r] = box(((R) rec).c0());
    //         ...
    //     }
    // }
    private static void genDrain(ClassWriter cw, String recordName, RecordComponent[] comps) {
        final int numRows = 1;
        final int records = 2;
        final int perField = 3;
        final int r = 4;
        final int rec = 5;
        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "drain", "(I[Ljava/lang/Object;[[Ljava/lang/Object;)V",
                null, null);
        mv.visitCode();
        Label loop = new Label();
        Label next = new Label();
        Label end = new Label();
        mv.visitInsn(ICONST_0);
        mv.visitVarInsn(ISTORE, r);
        mv.visitLabel(loop);
        mv.visitVarInsn(ILOAD, r);
        mv.visitVarInsn(ILOAD, numRows);
        mv.visitJumpInsn(IF_ICMPGE, end);
        mv.visitVarInsn(ALOAD, records);
        mv.visitVarInsn(ILOAD, r);
        mv.visitInsn(AALOAD);
        mv.visitVarInsn(ASTORE, rec);
        mv.visitVarInsn(ALOAD, rec);
        mv.visitJumpInsn(IFNULL, next);
        for (int f = 0; f < comps.length; f++) {
            Class<?> type = comps[f].getType();
            mv.visitVarInsn(ALOAD, perField);
            pushInt(mv, f);
            mv.visitInsn(AALOAD);
            mv.visitVarInsn(ILOAD, r);
            mv.visitVarInsn(ALOAD, rec);
            mv.visitTypeInsn(CHECKCAST, recordName);
            mv.visitMethodInsn(INVOKEVIRTUAL, recordName, comps[f].getAccessor().getName(),
                    Type.getMethodDescriptor(Type.getType(type)), false);
            if (type.isPrimitive()) {
                box(mv, type);
            }
            mv.visitInsn(AASTORE);
        }
        mv.visitLabel(next);
        mv.visitIincInsn(r, 1);
        mv.visitJumpInsn(GOTO, loop);
        mv.visitLabel(end);
        mv.visitInsn(RETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    // public Object[] build(int numRows, byte[] nulls, Object[] fieldArrays) {
    //     R[] out = new R[numRows];
    //     for (int r = 0; r < numRows; r++) {
    //         if (nulls != null && nulls[r] != 0) continue;
    //         out[r] = new R(unbox((T0) ((Object[]) fieldArrays[0])[r]), ...);
    //     }
    //     return out;
    // }
    private static void genBuild(ClassWriter cw, String recordName, RecordComponent[] comps) {
        final int numRows = 1;
        final int nulls = 2;
        final int fieldArrays = 3;
        final int out = 4;
        final int r = 5;
        MethodVisitor mv = cw.visitMethod(ACC_PUBLIC, "build", "(I[B[Ljava/lang/Object;)[Ljava/lang/Object;",
                null, null);
        mv.visitCode();
        mv.visitVarInsn(ILOAD, numRows);
        mv.visitTypeInsn(ANEWARRAY, recordName);
        mv.visitVarInsn(ASTORE, out);
        Label loop = new Label();
        Label notNull = new Label();
        Label next = new Label();
        Label end = new Label();
        mv.visitInsn(ICONST_0);
        mv.visitVarInsn(ISTORE, r);
        mv.visitLabel(loop);
        mv.visitVarInsn(ILOAD, r);
        mv.visitVarInsn(ILOAD, numRows);
        mv.visitJumpInsn(IF_ICMPGE, end);
        mv.visitVarInsn(ALOAD, nulls);
        mv.visitJumpInsn(IFNULL, notNull);
        mv.visitVarInsn(ALOAD, nulls);
        mv.visitVarInsn(ILOAD, r);
        mv.visitInsn(BALOAD);
        mv.visitJumpInsn(IFNE, next);
        mv.visitLabel(notNull);
        mv.visitVarInsn(ALOAD, out);
        mv.visitVarInsn(ILOAD, r);
        mv.visitTypeInsn(NEW, recordName);
        mv.visitInsn(DUP);
        Type[] paramTypes = new Type[comps.length];
        for (int f = 0; f < comps.length; f++) {
            Class<?> type = comps[f].getType();
            paramTypes[f] = Type.getType(type);
            mv.visitVarInsn(ALOAD, fieldArrays);
            pushInt(mv, f);
            mv.visitInsn(AALOAD);
            mv.visitTypeInsn(CHECKCAST, "[Ljava/lang/Object;");
            mv.visitVarInsn(ILOAD, r);
            mv.visitInsn(AALOAD);
            if (type.isPrimitive()) {
                unbox(mv, type);
            } else {
                mv.visitTypeInsn(CHECKCAST, Type.getInternalName(type));
            }
        }
        mv.visitMethodInsn(INVOKESPECIAL, recordName, "<init>",
                Type.getMethodDescriptor(Type.VOID_TYPE, paramTypes), false);
        mv.visitInsn(AASTORE);
        mv.visitLabel(next);
        mv.visitIincInsn(r, 1);
        mv.visitJumpInsn(GOTO, loop);
        mv.visitLabel(end);
        mv.visitVarInsn(ALOAD, out);
        mv.visitInsn(ARETURN);
        mv.visitMaxs(0, 0);
        mv.visitEnd();
    }

    private static void pushInt(MethodVisitor mv, int v) {
        if (v <= 5) {
            mv.visitInsn(ICONST_0 + v);
        } else if (v <= Byte.MAX_VALUE) {
            mv.visitIntInsn(BIPUSH, v);
        } else {
            mv.visitIntInsn(SIPUSH, v);
        }
    }

    private static Class<?> wrapperOf(Class<?> primitive) {
        return MethodType.methodType(primitive).wrap().returnType();
    }

    // primitive on stack -> Wrapper.valueOf(primitive)
    private static void box(MethodVisitor mv, Class<?> primitive) {
        Type wrapper = Type.getType(wrapperOf(primitive));
        mv.visitMethodInsn(INVOKESTATIC, wrapper.getInternalName(), "valueOf",
                Type.getMethodDescriptor(wrapper, Type.getType(primitive)), false);
    }

    // Object on stack -> ((Wrapper) o).xxxValue(); a null element throws NullPointerException, matching
    // Constructor.newInstance's IllegalArgumentException for a null primitive argument (both fail the row).
    private static void unbox(MethodVisitor mv, Class<?> primitive) {
        Type wrapper = Type.getType(wrapperOf(primitive));
        mv.visitTypeInsn(CHECKCAST, wrapper.getInternalName());
        mv.visitMethodInsn(INVOKEVIRTUAL, wrapper.getInternalName(), primitive.getName() + "Value",
                Type.getMethodDescriptor(Type.getType(primitive)), false);
    }
}
