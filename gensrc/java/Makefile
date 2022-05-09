# This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

BUILD_DIR = ${CURDIR}/../build/
SRC_DIR = ${CURDIR}
JAVAC = ${JAVA_HOME}/bin/javac -XDignore.symbol.file=true 
JAR = ${JAVA_HOME}/bin/jar


CALLSTUBGEN_SRC = ${SRC_DIR}/com/starrocks/udf/CallStubGenerator.java
CALLSTUBGEN_CLAZZ = ${SRC_DIR}/com/starrocks/udf/CallStubGenerator.class
ANALYZER_CLAZZ = ${BUILD_DIR}/gen_java/com/starrocks/udf/UDFClassAnalyzer.class
ANALYZER_SRC = ${SRC_DIR}/com/starrocks/udf/UDFClassAnalyzer.java
CLASSLOADER_CLAZZ = ${BUILD_DIR}/gen_java/com/starrocks/udf/UDFClassLoader.class
CLASSLOADER_SRC = ${SRC_DIR}/com/starrocks/udf/UDFClassLoader.java
UDFHELPER_CLAZZ= ${BUILD_DIR}/gen_java/com/starrocks/udf/UDFHelper.class
UDFHELPER_SRC = ${SRC_DIR}/com/starrocks/udf/UDFHelper.java

TARGET = ${BUILD_DIR}/gen_java/udf-class-loader.jar

all : ${TARGET}

${BUILD_DIR}/gen_java:
	mkdir -p $@

${CALLSTUBGEN_CLAZZ} : ${CALLSTUBGEN_SRC} ${CLASSLOADER_CLAZZ} ${BUILD_DIR}/gen_java
	${JAVAC} -d ${BUILD_DIR}/gen_java ${CALLSTUBGEN_SRC}

${ANALYZER_CLAZZ} : ${ANALYZER_SRC} ${BUILD_DIR}/gen_java
	${JAVAC} -d ${BUILD_DIR}/gen_java ${ANALYZER_SRC}

${CLASSLOADER_CLAZZ} : ${CLASSLOADER_SRC} ${BUILD_DIR}/gen_java
	${JAVAC} -d ${BUILD_DIR}/gen_java ${CLASSLOADER_SRC}

${UDFHELPER_CLAZZ} : ${UDFHELPER_SRC} ${BUILD_DIR}/gen_java
	${JAVAC} -d ${BUILD_DIR}/gen_java ${UDFHELPER_SRC}

${TARGET} : ${ANALYZER_CLAZZ} ${CLASSLOADER_CLAZZ} ${UDFHELPER_CLAZZ}
	cd ${BUILD_DIR}/gen_java && ${JAR} -cvf udf-class-loader.jar .


clean: 
	rm -rf ${BUILD_DIR}/gen_java
