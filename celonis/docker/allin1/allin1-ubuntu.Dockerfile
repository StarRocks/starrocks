ARG ARTIFACTIMAGE=starrocks-artifacts:ubuntu

FROM ${ARTIFACTIMAGE} as artifacts

FROM ubuntu:22.04 as dependencies-installed


RUN apt-get update -y \
        && apt-get install -y --no-install-recommends binutils-dev default-jdk python2 \
           mysql-client curl vim tree net-tools \
        && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/lib/jvm/default-java
ENV SR_HOME=/data/deploy/starrocks
# STARTMODE: [auto, manual]
ENV STARTMODE=manual

ARG DEPLOYDIR=/data/deploy

WORKDIR $DEPLOYDIR

# Copy all artifacts to the runtime container image
COPY --from=artifacts /release/be_artifacts/ $DEPLOYDIR/starrocks
COPY --from=artifacts /release/fe_artifacts/ $DEPLOYDIR/starrocks
COPY --from=artifacts /release/udf/ $DEPLOYDIR/starrocks/udf/

# Create directory for FE meta and BE storage in StarRocks.
RUN mkdir -p $DEPLOYDIR/starrocks/fe/meta && mkdir -p $DEPLOYDIR/starrocks/be/storage

# Copy Setup script.
COPY *.sh $DEPLOYDIR
RUN chmod +x *.sh

COPY *.conf $DEPLOYDIR
RUN cat be.conf >> $DEPLOYDIR/starrocks/be/conf/be.conf && \
    cat fe.conf >> $DEPLOYDIR/starrocks/fe/conf/fe.conf

CMD ./start_fe_be.sh
