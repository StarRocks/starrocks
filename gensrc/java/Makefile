# This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

BUILD_DIR = ${CURDIR}/../build/
SRC_DIR = ${CURDIR}
JAVAC = ${JAVA_HOME}/bin/javac
JAR = ${JAVA_HOME}/bin/jar


ANALYZER_CLAZZ = ${BUILD_DIR}/gen_java/com/starrocks/udf/UDFClassAnalyzer.class
ANALYZER_SRC = ${SRC_DIR}/com/starrocks/udf/UDFClassAnalyzer.java
CLASSLOADER_CLAZZ = ${BUILD_DIR}/gen_java/com/starrocks/udf/UDFClassLoader.class
CLASSLOADER_SRC = ${SRC_DIR}/com/starrocks/udf/UDFClassLoader.java
TARGET = ${BUILD_DIR}/gen_java/udf-class-loader.jar

all : ${TARGET}

${BUILD_DIR}/gen_java:
	mkdir -p $@

${ANALYZER_CLAZZ} : ${ANALYZER_SRC} ${BUILD_DIR}/gen_java
	${JAVAC} -d ${BUILD_DIR}/gen_java ${ANALYZER_SRC}

${CLASSLOADER_CLAZZ} : ${CLASSLOADER_SRC} ${BUILD_DIR}/gen_java
	${JAVAC} -d ${BUILD_DIR}/gen_java ${CLASSLOADER_SRC}

${TARGET} : ${ANALYZER_CLAZZ} ${CLASSLOADER_CLAZZ} 
	cd ${BUILD_DIR}/gen_java && jar -cvf udf-class-loader.jar .


clean: 
	rm -rf ${BUILD_DIR}/gen_java
