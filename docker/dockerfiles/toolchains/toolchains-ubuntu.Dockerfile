# Build toolchains on ubuntu22.04, dev-env image can be built based on this image for ubuntu22
#  DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/toolchains/toolchains-ubuntu.Dockerfile -t toolchains-ubuntu:latest docker/dockerfiles/toolchains/

FROM ubuntu:22.04

# Install common libraries and tools that are needed for dev environment
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y \
    automake binutils-dev bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 make cmake gcc-12 g++-12 openjdk-17-jdk git patch lld bzip2 \
    wget unzip curl vim tree net-tools openssh-client xz-utils gh locales && \
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata && \
    locale-gen en_US.UTF-8 && \
    rm -rf /var/lib/apt/lists/*
RUN ln -s -f /usr/bin/gcc-12 /usr/bin/gcc && ln -s -f /usr/bin/g++-12 /usr/bin/g++

# Set the soft link to jvm
RUN ARCH=`uname -m` && \
    cd /lib/jvm && \
    if [ "$ARCH" = "aarch64" ] ; then ln -s java-17-openjdk-arm64 java-17-openjdk ; else ln -s java-17-openjdk-amd64 java-17-openjdk  ; fi ;

ENV JAVA_HOME=/lib/jvm/java-17-openjdk
ENV STARROCKS_LINKER=lld
ENV LANG=en_US.utf8
