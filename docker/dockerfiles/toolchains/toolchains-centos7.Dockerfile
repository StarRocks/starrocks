# Build toolchains on centos7, dev-env image can be built based on this image for centos7
#  DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/toolchains/toolchains-centos7.Dockerfile -t toolchains-centos7:latest docker/dockerfiles/toolchains/

ARG GCC_INSTALL_HOME=/opt/rh/gcc-toolset-10/root/usr
ARG GCC_DOWNLOAD_URL=https://ftp.gnu.org/gnu/gcc/gcc-10.3.0/gcc-10.3.0.tar.gz
ARG CMAKE_INSTALL_HOME=/opt/cmake
ARG MAVEN_VERSION=3.6.3
ARG MAVEN_INSTALL_HOME=/opt/maven
# Can't upgrade to a later version, due to incompatible changes between 2.31 and 2.32
ARG BINUTILS_DOWNLOAD_URL=https://ftp.gnu.org/gnu/binutils/binutils-2.30.tar.bz2

FROM centos:centos7 as base-builder
RUN yum install -y gcc gcc-c++ make automake curl wget gzip gunzip zip bzip2 file texinfo && yum clean metadata


FROM base-builder as gcc-builder
ARG GCC_INSTALL_HOME
ARG GCC_DOWNLOAD_URL
RUN mkdir -p /workspace/gcc && \
    cd /workspace/gcc &&    \
    wget --no-check-certificate $GCC_DOWNLOAD_URL -O ../gcc.tar.gz && \
    tar -xzf ../gcc.tar.gz --strip-components=1 && \
    ./contrib/download_prerequisites && \
    ./configure --disable-multilib --enable-languages=c,c++ --prefix=${GCC_INSTALL_HOME}
RUN cd /workspace/gcc && make -j`nproc`
RUN cd /workspace/gcc && mkdir -p /workspace/installed && make DESTDIR=/workspace/installed install && \
    strip /workspace/installed/${GCC_INSTALL_HOME}/bin/* /workspace/installed/${GCC_INSTALL_HOME}/libexec/gcc/*/*/{cc1,cc1plus,collect2,lto1}


FROM base-builder as binutils-builder
ARG BINUTILS_DOWNLOAD_URL
# build binutils and only install gnu as
RUN mkdir -p /workspace/binutils && \
    cd /workspace/binutils && \
    wget --no-check-certificate $BINUTILS_DOWNLOAD_URL -O ../binutils.tar.bz2 && \
    tar -xjf ../binutils.tar.bz2 --strip-components=1 && \
    ./configure --prefix=/usr   && \
    make -j `nproc` && \
    mkdir -p /workspace/installed  && cd gas && make DESTDIR=/workspace/installed install

FROM centos:centos7

ARG GCC_INSTALL_HOME
ARG CMAKE_INSTALL_HOME
ARG MAVEN_VERSION
ARG MAVEN_INSTALL_HOME

LABEL org.opencontainers.image.source="https://github.com/starrocks/starrocks"

RUN yum install -y epel-release && yum install -y wget unzip bzip2 patch bison byacc flex autoconf automake make \
        libtool which git ccache binutils-devel python3 file java-11-openjdk java-11-openjdk-devel java-11-openjdk-jmods less psmisc && \
        yum clean all && rm -rf /var/cache/yum

# install gcc
COPY --from=gcc-builder /workspace/installed/ /
# install binutils
COPY --from=binutils-builder /workspace/installed/ /
# install cmake
RUN ARCH=`uname -m` && mkdir -p $CMAKE_INSTALL_HOME && cd $CMAKE_INSTALL_HOME && \
    curl -s -k https://cmake.org/files/v3.22/cmake-3.22.4-linux-${ARCH}.tar.gz | tar -xzf - --strip-components=1 && \
    ln -s $CMAKE_INSTALL_HOME/bin/cmake /usr/bin/cmake
# install maven
RUN mkdir -p ${MAVEN_INSTALL_HOME} && cd ${MAVEN_INSTALL_HOME} && \
    curl -s -k https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz | tar -xzf - --strip-components=1 && \
    ln -s ${MAVEN_INSTALL_HOME}/bin/mvn /usr/bin/mvn
# install clang-format
RUN ARCH=`uname -m` ; if [[ $ARCH == "aarch64" ]] ; then DOWNLOAD_URL=http://cdn-thirdparty.starrocks.com/aarch64/clang-format ; else DOWNLOAD_URL=http://cdn-thirdparty.starrocks.com/clang-format ; fi ; curl -s $DOWNLOAD_URL -o /usr/bin/clang-format && chmod +x /usr/bin/clang-format

ENV STARROCKS_GCC_HOME=${GCC_INSTALL_HOME}
ENV JAVA_HOME=/usr/lib/jvm/java-11
ENV MAVEN_HOME=${MAVEN_INSTALL_HOME}
