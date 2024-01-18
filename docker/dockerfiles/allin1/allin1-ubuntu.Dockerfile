# This docker file build the Starrocks allin1 ubuntu image
# Please run this command from the git repo root directory to build:
#
#   - Use artifact image to package runtime container:
<<<<<<< HEAD
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=ghcr.io/starrocks/starrocks/artifact-ubuntu:latest -f docker/dockerfiles/allin1/allin1-ubuntu.Dockerfile -t allin1-ubuntu:latest .
=======
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=starrocks/artifacts-ubuntu:latest -f docker/dockerfiles/allin1/allin1-ubuntu.Dockerfile -t allin1-ubuntu:latest .
>>>>>>> 2.5.18
#   - Use locally build artifacts to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=. -f docker/dockerfiles/allin1/allin1-ubuntu.Dockerfile -t allin1-ubuntu:latest .
#
# The artifact source used for packing the runtime docker image
#   image: copy the artifacts from a artifact docker image.
#   local: copy the artifacts from a local repo. Mainly used for local development and test.
ARG ARTIFACT_SOURCE=image

ARG ARTIFACTIMAGE=artifact:latest
FROM ${ARTIFACTIMAGE} as artifacts-from-image

# create a docker build stage that copy locally build artifacts
FROM busybox:latest as artifacts-from-local
ARG LOCAL_REPO_PATH

COPY ${LOCAL_REPO_PATH}/output/fe /release/fe_artifacts/fe
COPY ${LOCAL_REPO_PATH}/output/be /release/be_artifacts/be
COPY ${LOCAL_REPO_PATH}/fs_brokers/apache_hdfs_broker/output/apache_hdfs_broker /release/broker_artifacts/apache_hdfs_broker


FROM artifacts-from-${ARTIFACT_SOURCE} as artifacts
RUN rm -f /release/be_artifacts/be/lib/starrocks_be.debuginfo


FROM ubuntu:22.04 as dependencies-installed
ARG DEPLOYDIR=/data/deploy
ENV SR_HOME=${DEPLOYDIR}/starrocks

RUN apt-get update -y && apt-get install -y --no-install-recommends \
<<<<<<< HEAD
        binutils-dev default-jdk python2 mysql-client curl vim tree net-tools less tzdata linux-tools-common linux-tools-generic && \
=======
        binutils-dev default-jdk python2 mysql-client curl vim tree net-tools less tzdata linux-tools-common linux-tools-generic supervisor nginx netcat && \
>>>>>>> 2.5.18
        ln -fs /usr/share/zoneinfo/UTC /etc/localtime && \
        dpkg-reconfigure -f noninteractive tzdata && \
        rm -rf /var/lib/apt/lists/*
RUN echo "export PATH=/usr/lib/linux-tools/5.15.0-60-generic:$PATH" >> /etc/bash.bashrc
ENV JAVA_HOME=/lib/jvm/default-java

WORKDIR $DEPLOYDIR

# Copy all artifacts to the runtime container image
COPY --from=artifacts /release/be_artifacts/ $DEPLOYDIR/starrocks
COPY --from=artifacts /release/fe_artifacts/ $DEPLOYDIR/starrocks
COPY --from=artifacts /release/broker_artifacts/ $DEPLOYDIR/starrocks

<<<<<<< HEAD
# Copy Setup script.
COPY --chmod=755 docker/dockerfiles/allin1/*.sh $DEPLOYDIR

# Copy config files
COPY docker/dockerfiles/allin1/*.conf $DEPLOYDIR
RUN cat be.conf >> $DEPLOYDIR/starrocks/be/conf/be.conf && \
    cat fe.conf >> $DEPLOYDIR/starrocks/fe/conf/fe.conf && \
    mkdir -p $DEPLOYDIR/starrocks/fe/meta && mkdir -p $DEPLOYDIR/starrocks/be/storage && touch /.dockerenv
=======
# Copy setup script and config files
COPY docker/dockerfiles/allin1/*.sh docker/dockerfiles/allin1/*.conf docker/dockerfiles/allin1/*.txt $DEPLOYDIR
COPY docker/dockerfiles/allin1/services/ $SR_HOME

RUN cat be.conf >> $DEPLOYDIR/starrocks/be/conf/be.conf && \
    cat fe.conf >> $DEPLOYDIR/starrocks/fe/conf/fe.conf && \
    rm -f be.conf fe.conf && \
    mkdir -p $DEPLOYDIR/starrocks/fe/meta $DEPLOYDIR/starrocks/be/storage && touch /.dockerenv
>>>>>>> 2.5.18

CMD ./entrypoint.sh
