# Build toolchains on centos7, dev-env image can be built based on this image for centos7
#  DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/toolchains/centos7-toolchains.Dockerfile -t starrocks/centos7-toolchains:20230306 docker/dockerfiles/centos7-toolchains/

ARG GCC_INSTALL_HOME=/opt/gcc/usr
ARG GCC_DOWNLOAD_URL=https://ftp.gnu.org/gnu/gcc/gcc-10.3.0/gcc-10.3.0.tar.gz
ARG CMAKE_INSTALL_HOME=/opt/cmake
ARG MAVEN_VERSION=3.6.3
ARG MAVEN_INSTALL_HOME=/opt/maven

FROM centos:centos7 as gcc-builder
ARG GCC_INSTALL_HOME
ARG GCC_DOWNLOAD_URL

RUN yum install -y gcc gcc-c++ make automake curl wget gzip gunzip zip bzip2 file texinfo && yum clean metadata
RUN mkdir -p /workspace/gcc && \
    cd /workspace/gcc &&    \
    wget --no-check-certificate $GCC_DOWNLOAD_URL -O ../gcc.tar.gz && \
    tar -xzf ../gcc.tar.gz --strip-components=1 && \
    ./contrib/download_prerequisites && \
    ./configure --disable-multilib --enable-languages=c,c++ --prefix=${GCC_INSTALL_HOME}
RUN cd /workspace/gcc && make -j`nproc`
RUN cd /workspace/gcc && mkdir -p /workspace/installed && make DESTDIR=/workspace/installed install && \
    strip /workspace/installed/${GCC_INSTALL_HOME}/bin/* /workspace/installed/${GCC_INSTALL_HOME}/libexec/gcc/*/*/{cc1,cc1plus,collect2,lto1}


FROM centos:centos7

ARG GCC_INSTALL_HOME
ARG CMAKE_INSTALL_HOME
ARG MAVEN_VERSION
ARG MAVEN_INSTALL_HOME
ARG COMMIT_ID

LABEL org.opencontainers.image.source="https://github.com/starrocks/starrocks"
LABEL com.starrocks.version=${COMMIT_ID:-"UNKNOWN"}

RUN yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel && yum clean all && rm -rf /var/cache/yum

# install gcc
COPY --from=gcc-builder /workspace/installed/ /
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
ENV JAVA_HOME=/usr/lib/jvm/java
ENV MAVEN_HOME=${MAVEN_INSTALL_HOME}
