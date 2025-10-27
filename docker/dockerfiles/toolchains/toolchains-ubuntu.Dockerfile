# Build toolchains on ubuntu22.04, dev-env image can be built based on this image for ubuntu22
#  DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/toolchains/toolchains-ubuntu.Dockerfile -t toolchains-ubuntu:latest docker/dockerfiles/toolchains/

ARG GCC_INSTALL_HOME=/opt/gcc-toolset-14
ARG GCC_WORK_DIR=/workspace/gcc-14
ARG GCC_DOWNLOAD_URL=https://ftp.gnu.org/gnu/gcc/gcc-14.3.0/gcc-14.3.0.tar.gz

FROM ubuntu:22.04 AS build-gcc

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential wget curl git bison flex texinfo gawk libgmp-dev libmpfr-dev libmpc-dev zlib1g-dev && \
    apt-get install -y ca-certificates && update-ca-certificates

RUN mkdir -p $GCC_WORK_DIR && \
    cd $GCC_WORK_DIR && \
    wget --no-check-certificate $GCC_DOWNLOAD_URL -O ../gcc.tar.gz && \
    tar -zxf ../gcc.tar.gz --strip-components=1 && \
    ./contrib/download_prerequisites && \
    ./configure --disable-multilib --enable-languages=c,c++ --prefix=$GCC_INSTALL_HOME && \
    make -j$(nproc) && \
    make install-strip

FROM ubuntu:22.04

ARG GCC_INSTALL_HOME
ARG CMAKE_INSTALL_HOME=/opt/cmake
ARG COMMIT_ID=unset

LABEL org.opencontainers.image.source="https://github.com/StarRocks/starrocks"
LABEL com.starrocks.commit=${COMMIT_ID}

# Install common libraries and tools that are needed for dev environment
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y \
    automake binutils-dev bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 make openjdk-17-jdk git patch lld bzip2 \
    wget unzip curl vim tree net-tools openssh-client xz-utils gh locales && \
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata && \
    locale-gen en_US.UTF-8 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build-gcc $GCC_INSTALL_HOME $GCC_INSTALL_HOME

RUN for bin in cpp gcc g++ gcc-ar gcc-nm gcov gcov-dump gcov-tool lto-dump; do \
        update-alternatives --install /usr/bin/$bin $bin ${GCC_INSTALL_HOME}/bin/$bin 200; \
    done && \
    update-alternatives --set gcc ${GCC_INSTALL_HOME}/bin/gcc && \
    update-alternatives --set g++ ${GCC_INSTALL_HOME}/bin/g++ && \
    update-alternatives --set cpp ${GCC_INSTALL_HOME}/bin/cpp

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
