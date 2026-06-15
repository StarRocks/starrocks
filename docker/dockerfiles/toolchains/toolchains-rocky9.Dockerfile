# Build toolchains on rockylinux9, dev-env image can be built based on this image for rocky9
# NOTE: build context MUST be set to `docker/dockerfiles/toolchains/`
#  DOCKER_BUILDKIT=1 docker build --rm=true -f docker/dockerfiles/toolchains/toolchains-rocky9.Dockerfile -t toolchains-rocky9:latest docker/dockerfiles/toolchains/

ARG GCC_INSTALL_HOME=/opt/gcc-toolset-14
ARG GCC_DOWNLOAD_URL=https://ftp.gnu.org/gnu/gcc/gcc-14.3.0/gcc-14.3.0.tar.gz
ARG CMAKE_INSTALL_HOME=/opt/cmake
ARG MAVEN_VERSION=3.6.3
ARG MAVEN_INSTALL_HOME=/opt/maven
ARG COMMIT_ID=unset


FROM rockylinux:9 AS base-builder
# Rocky9 ships a recent gcc and binutils, enough to build gcc-14 directly.
# curl is intentionally omitted: the base image already ships curl-minimal, and installing the
# full curl package conflicts with it. Downloads here use wget.
RUN dnf install -y gcc gcc-c++ make automake wget gzip zip bzip2 file bison flex diffutils && \
    dnf clean all


FROM base-builder AS gcc-builder
ARG GCC_INSTALL_HOME
ARG GCC_DOWNLOAD_URL
# build gcc-14 with the distro gcc
RUN mkdir -p /workspace/gcc && \
    cd /workspace/gcc &&    \
    wget --progress=dot:mega --no-check-certificate $GCC_DOWNLOAD_URL -O ../gcc.tar.gz && \
    tar -xzf ../gcc.tar.gz --strip-components=1 && \
    ./contrib/download_prerequisites && \
    ./configure --disable-multilib --enable-languages=c,c++ --prefix=${GCC_INSTALL_HOME}
RUN cd /workspace/gcc && make -j`nproc`
RUN cd /workspace/gcc && mkdir -p /workspace/installed && make DESTDIR=/workspace/installed install && \
    strip /workspace/installed/${GCC_INSTALL_HOME}/bin/* /workspace/installed/${GCC_INSTALL_HOME}/libexec/gcc/*/*/{cc1,cc1plus,collect2,lto1}


FROM rockylinux:9

ARG GCC_INSTALL_HOME
ARG CMAKE_INSTALL_HOME
ARG MAVEN_VERSION
ARG MAVEN_INSTALL_HOME
ARG COMMIT_ID

LABEL org.opencontainers.image.source="https://github.com/StarRocks/starrocks"
LABEL com.starrocks.commit=${COMMIT_ID}

# Note: these packages are not part of rocky9's minimal base image, but are required to build
# StarRocks: xz for .tar.xz extraction, gettext for autotools, perl (FindBin and friends) for
# OpenSSL's Configure script, and binutils-devel for libiberty.a needed when linking the backend.
RUN dnf install -y 'dnf-command(config-manager)' && \
        dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo && \
        dnf install -y epel-release && \
        dnf install -y gh wget unzip bzip2 xz patch bison byacc flex autoconf automake make \
        gettext perl binutils-devel libtool which git ccache python3 java-17-openjdk-devel file less psmisc clang-tools-extra glibc-langpack-en && \
        dnf clean all && rm -rf /var/cache/dnf

# install gcc
COPY --from=gcc-builder /workspace/installed/ /
# install cmake
RUN ARCH=`uname -m` && mkdir -p $CMAKE_INSTALL_HOME && cd $CMAKE_INSTALL_HOME && \
    curl -s -k https://cmake.org/files/v3.31/cmake-3.31.9-linux-${ARCH}.tar.gz | tar -xzf - --strip-components=1 && \
    ln -s $CMAKE_INSTALL_HOME/bin/cmake /usr/bin/cmake

# jdk17 is provided by the distro `java-17-openjdk-devel` package (installed above), no external download needed

# install maven
RUN mkdir -p ${MAVEN_INSTALL_HOME} && cd ${MAVEN_INSTALL_HOME} && \
    curl -s -k https://archive.apache.org/dist/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz | tar -xzf - --strip-components=1 && \
    ln -s ${MAVEN_INSTALL_HOME}/bin/mvn /usr/bin/mvn
# clang-format is provided by the distro `clang-tools-extra` package (installed above), no external download needed

ENV STARROCKS_GCC_HOME=${GCC_INSTALL_HOME}
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk
ENV MAVEN_HOME=${MAVEN_INSTALL_HOME}
ENV LANG=en_US.utf8
