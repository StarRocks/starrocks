# This docker file build the Starrocks be ubuntu image
# Please run this command from the git repo root directory to build:
#
#   - Use artifact image to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=starrocks/artifacts-ubuntu:latest -f docker/dockerfiles/be/be-ubuntu.Dockerfile -t be-ubuntu:latest .
#   - Use locally build artifacts to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=. -f docker/dockerfiles/be/be-ubuntu.Dockerfile -t be-ubuntu:latest .
#
# The artifact source used for packing the runtime docker image
#   image: copy the artifacts from a artifact docker image.
#   local: copy the artifacts from a local repo. Mainly used for local development and test.
ARG ARTIFACT_SOURCE=image
# The default run_as user when starting the container
ARG RUN_AS_USER=root
# The precreated non-privileged user account, the owner of the starrocks assets
ARG USER=starrocks


ARG ARTIFACTIMAGE=starrocks/artifacts-ubuntu:latest
FROM ${ARTIFACTIMAGE} as artifacts-from-image

# create a docker build stage that copy locally build artifacts
FROM busybox:latest as artifacts-from-local
ARG LOCAL_REPO_PATH
COPY ${LOCAL_REPO_PATH}/output/be /release/be_artifacts/be


FROM artifacts-from-${ARTIFACT_SOURCE} as artifacts
RUN rm -f /release/be_artifacts/be/lib/starrocks_be.debuginfo


FROM ubuntu:22.04
ARG STARROCKS_ROOT=/opt/starrocks
ARG USER
ARG RUN_AS_USER
ARG GROUP=starrocks

RUN apt-get update -y && apt-get install -y --no-install-recommends \
        binutils-dev openjdk-17-jdk mysql-client curl vim tree net-tools less tzdata locales pigz inotify-tools rclone gdb && \
        ln -fs /usr/share/zoneinfo/UTC /etc/localtime && \
        dpkg-reconfigure -f noninteractive tzdata && \
        locale-gen en_US.UTF-8 && \
        rm -rf /var/lib/apt/lists/*
RUN touch /.dockerenv ; ARCH=`uname -m` && cd /lib/jvm && \
    if [ "$ARCH" = "aarch64" ] ; then ln -s java-17-openjdk-arm64 java-17-openjdk ; else ln -s java-17-openjdk-amd64 java-17-openjdk  ; fi ;
ENV JAVA_HOME=/lib/jvm/java-17-openjdk

WORKDIR $STARROCKS_ROOT

RUN groupadd --gid 1000 $GROUP && \
    if [ "$USER" != "root" ]; then \
        useradd --no-create-home --uid 1000 --gid 1000 --shell /usr/sbin/nologin $USER; \
    fi && \
    chown -R $USER:$GROUP $STARROCKS_ROOT

USER $USER

# Copy all artifacts to the runtime container image
COPY --from=artifacts --chown=$USER:$GROUP /release/be_artifacts/ $STARROCKS_ROOT/

# Copy be k8s scripts to the runtime container image
COPY --chown=$USER:$GROUP docker/dockerfiles/be/*.sh $STARROCKS_ROOT/

# Create directory for BE storage, create cn symbolic link to be
RUN mkdir -p $STARROCKS_ROOT/be/storage && ln -sfT be $STARROCKS_ROOT/cn

USER $RUN_AS_USER
