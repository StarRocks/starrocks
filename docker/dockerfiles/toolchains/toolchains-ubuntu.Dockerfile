# Build toolchains on ubuntu22.04, dev-env image can be built based on this image for ubuntu22
#  DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/toolchains/toolchains-ubuntu.Dockerfile -t toolchains-ubuntu:latest docker/dockerfiles/toolchains/

FROM ubuntu:22.04

# Install common libraries and tools that are needed for dev environment
RUN apt-get update -y && \
    apt-get install --no-install-recommends -y \
    automake binutils-dev bison byacc ccache flex libiberty-dev libtool maven zip python3 python-is-python3 make cmake gcc g++ default-jdk git patch lld bzip2 \
    wget unzip curl vim tree net-tools openssh-client && \
    DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get -y install tzdata && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/lib/jvm/default-java
ENV STARROCKS_LINKER=lld
