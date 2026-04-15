#!/usr/bin/env bash
# Builds libadbc_driver_jni.so from arrow-adbc source and repackages the upstream
# Maven JAR. Called by maven-antrun-plugin during 'mvn package'. All paths are
# passed as environment variables by the pom.xml.
#
# Required env vars: DL_DIR, SRC_DIR, BUILD_DIR, REPACK_DIR, M2_REPO,
#                    ADBC_VERSION, TARBALL_PREFIX, JAVA_HOME
set -euo pipefail

echo "=== Extracting source files ==="
cd "$SRC_DIR"
tar xzf "$DL_DIR/arrow-adbc.tar.gz" --strip-components=1 \
    "$TARBALL_PREFIX/java/driver/jni/src/main/cpp/jni_wrapper.cc" \
    "$TARBALL_PREFIX/java/driver/jni/src/main/java/org/apache/arrow/adbc/driver/jni/impl/" \
    "$TARBALL_PREFIX/c/driver_manager/adbc_driver_manager.cc" \
    "$TARBALL_PREFIX/c/driver_manager/current_arch.h" \
    "$TARBALL_PREFIX/c/include/arrow-adbc/adbc.h" \
    "$TARBALL_PREFIX/c/include/arrow-adbc/adbc_driver_manager.h"

echo "=== Generating JNI header ==="
mkdir -p "$SRC_DIR/jni-headers" "$SRC_DIR/jni-gen"
JARS="$M2_REPO/org/apache/arrow/adbc/adbc-core/$ADBC_VERSION/adbc-core-$ADBC_VERSION.jar"
JARS="$JARS:$M2_REPO/org/apache/arrow/arrow-c-data/18.0.0/arrow-c-data-18.0.0.jar"
JARS="$JARS:$M2_REPO/org/apache/arrow/arrow-memory-core/18.0.0/arrow-memory-core-18.0.0.jar"
IMPL_DIR="$SRC_DIR/java/driver/jni/src/main/java/org/apache/arrow/adbc/driver/jni/impl"
javac -h "$SRC_DIR/jni-headers" -d "$SRC_DIR/jni-gen" -cp "$JARS" \
    "$IMPL_DIR"/*.java

echo "=== Setting up toml++ ==="
mkdir -p "$SRC_DIR/toml-include/toml++"
cp "$DL_DIR/toml.hpp" "$SRC_DIR/toml-include/toml++/"

echo "=== Compiling libadbc_driver_jni.so ==="
g++ -shared -fPIC -O2 -std=c++17 \
    -I"$SRC_DIR/c/include" \
    -I"$SRC_DIR/c/driver_manager" \
    -I"$SRC_DIR/jni-headers" \
    -I"$SRC_DIR/toml-include" \
    -I"$JAVA_HOME/include" \
    -I"$JAVA_HOME/include/linux" \
    -o "$BUILD_DIR/libadbc_driver_jni.so" \
    "$SRC_DIR/java/driver/jni/src/main/cpp/jni_wrapper.cc" \
    "$SRC_DIR/c/driver_manager/adbc_driver_manager.cc" \
    -static-libstdc++ -static-libgcc \
    -ldl -lpthread

echo "=== Verifying GLIBCXX ==="
GLIBCXX_COUNT=$(strings "$BUILD_DIR/libadbc_driver_jni.so" | grep -c '^GLIBCXX_' || true)
if [ "$GLIBCXX_COUNT" -gt 0 ]; then
    echo "FAIL: .so has dynamic GLIBCXX dependency"
    strings "$BUILD_DIR/libadbc_driver_jni.so" | grep '^GLIBCXX_'
    exit 1
fi
echo "OK: no dynamic GLIBCXX dependency (static-linked)"

echo "=== Repackaging JAR ==="
mkdir -p "$REPACK_DIR"
cd "$REPACK_DIR"
jar xf "$M2_REPO/org/apache/arrow/adbc/adbc-driver-jni/$ADBC_VERSION/adbc-driver-jni-$ADBC_VERSION.jar"
cp "$BUILD_DIR/libadbc_driver_jni.so" adbc_driver_jni/x86_64/libadbc_driver_jni.so
jar cf "$BUILD_DIR/adbc-driver-jni-$ADBC_VERSION.jar" .

echo "=== Installing to local Maven repo ==="
mvn -q install:install-file \
    -Dfile="$BUILD_DIR/adbc-driver-jni-$ADBC_VERSION.jar" \
    -DgroupId=org.apache.arrow.adbc \
    -DartifactId=adbc-driver-jni \
    -Dversion="$ADBC_VERSION" \
    -Dpackaging=jar \
    -DgeneratePom=false

echo "=== Done ==="
