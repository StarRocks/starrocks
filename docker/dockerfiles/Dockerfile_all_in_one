ARG BRANCH=main
From starrocks/dev-env:${BRANCH} as builder

Add . /build/starrocks
Workdir /build/starrocks

RUN longver=`git describe --tags --long --always` && \
    version="${longver%-*}" && \
    STARROCKS_VERSION=${version%-0} BUILD_TYPE="Release" ./build.sh --fe --be --clean

RUN mkdir output/be/storage output/fe/meta
RUN strip output/be/lib/starrocks_be
RUN rm -rf output/be/lib/jvm

From centos:7
RUN yum install -y java-devel
ENV JAVA_HOME=/usr/lib/jvm/java
Copy --from=builder /build/starrocks/output /root/starrocks
Workdir /root/starrocks
