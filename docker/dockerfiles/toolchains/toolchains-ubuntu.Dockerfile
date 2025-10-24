# Build toolchains on ubuntu22.04, dev-env image can be built based on this image for ubuntu22
#  DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/toolchains/toolchains-ubuntu.Dockerfile -t toolchains-ubuntu:latest docker/dockerfiles/toolchains/

FROM ubuntu:22.04
ARG CMAKE_INSTALL_HOME=/opt/cmake
ARG COMMIT_ID=unset

LABEL org.opencontainers.image.source="https://github.com/StarRocks/starrocks"
LABEL com.starrocks.commit=${COMMIT_ID}

# Install common libraries and tools that are needed for dev environment
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y \
    automake binutils-dev bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 make gcc-12 g++-12 openjdk-17-jdk git patch lld bzip2 \
    wget unzip curl vim tree net-tools openssh-client xz-utils gh locales && \
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata && \
    locale-gen en_US.UTF-8 && \
    rm -rf /var/lib/apt/lists/*
RUN ln -s -f /usr/bin/cpp-12 /usr/bin/cpp && \
    ln -s -f /usr/bin/g++-12 /usr/bin/g++ && \
    ln -s -f /usr/bin/gcc-12 /usr/bin/gcc && \
    ln -s -f /usr/bin/gcc-ar-12 /usr/bin/gcc-ar && \
    ln -s -f /usr/bin/gcc-nm-12 /usr/bin/gcc-nm && \
    ln -s -f /usr/bin/gcc-ranlib-12 /usr/bin/gcc-ranlib && \
    ln -s -f /usr/bin/gcov-12 /usr/bin/gcov && \
    ln -s -f /usr/bin/gcov-dump-12 /usr/bin/gcov-dump && \
    ln -s -f /usr/bin/gcov-tool-12 /usr/bin/gcov-tool && \
    ln -s -f /usr/bin/lto-dump-12 /usr/bin/lto-dump  

# install cmake
RUN ARCH=`uname -m` && mkdir -p $CMAKE_INSTALL_HOME && cd $CMAKE_INSTALL_HOME && \
    curl -s -k https://cmake.org/files/v3.31/cmake-3.31.9-linux-${ARCH}.tar.gz | tar -xzf - --strip-components=1 && \
    ln -s $CMAKE_INSTALL_HOME/bin/cmake /usr/bin/cmake

# Set the soft link to jvm
RUN ARCH=`uname -m` && \
    cd /lib/jvm && \
    if [ "$ARCH" = "aarch64" ] ; then ln -s java-17-openjdk-arm64 java-17-openjdk ; else ln -s java-17-openjdk-amd64 java-17-openjdk  ; fi ;

ENV JAVA_HOME=/lib/jvm/java-17-openjdk
ENV STARROCKS_LINKER=lld
ENV LANG=en_US.utf8
