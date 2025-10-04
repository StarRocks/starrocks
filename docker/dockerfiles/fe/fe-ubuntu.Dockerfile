# This docker file build the Starrocks fe ubuntu image
# Please run this command from the git repo root directory to build:
#   - Use artifact image to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=starrocks/artifacts-ubuntu:latest -f docker/dockerfiles/fe/fe-ubuntu.Dockerfile -t fe-ubuntu:latest .
#   - Use locally build artifacts to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=. -f docker/dockerfiles/fe/fe-ubuntu.Dockerfile -t fe-ubuntu:latest .
#   - Build the minimal version of the image
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=starrocks/artifacts-ubuntu:latest --build-arg MINIMAL=true -f docker/dockerfiles/fe/fe-ubuntu.Dockerfile -t fe-ubuntu-mininal:latest .
#
# The artifact source used for packing the runtime docker image
#   image: copy the artifacts from a artifact docker image.
#   local: copy the artifacts from a local repo. Mainly used for local development and test.
ARG ARTIFACT_SOURCE=image
# The default run_as user when starting the container
ARG RUN_AS_USER=root
# The precreated non-privileged user account, the owner of the starrocks assets
ARG USER=starrocks
# Build the minimal version of image, MINIMAL={true|false}
# NOTE:
# - if MINIMAL=true, RUN_AS_USER parameter will take no effect, the USER for the container will be set to $USER forcibly
# TODO: make MINIMAL=true as the default behavior
ARG MINIMAL=false


ARG ARTIFACTIMAGE=starrocks/artifacts-ubuntu:latest
FROM ${ARTIFACTIMAGE} as artifacts-from-image

# create a docker build stage that copy locally build artifacts
FROM busybox:latest as artifacts-from-local
ARG LOCAL_REPO_PATH
COPY ${LOCAL_REPO_PATH}/output/fe /release/fe_artifacts/fe


FROM artifacts-from-${ARTIFACT_SOURCE} as artifacts


FROM ubuntu:22.04 AS base_image
ARG STARROCKS_ROOT=/opt/starrocks
ARG USER
ARG RUN_AS_USER
ARG GROUP=starrocks
ARG MINIMAL

# TODO: switch to `openjdk-##-jre` when the starrocks core is ready.
RUN OPTIONAL_PKGS="" && if [ "x$MINIMAL" = "xfalse" ] ; then OPTIONAL_PKGS="openjdk-17-jdk curl vim tree net-tools less pigz rclone" ; fi && \
        apt-get update -y && apt-get install -y --no-install-recommends \
        openjdk-17-jdk mysql-client tzdata locales netcat tini $OPTIONAL_PKGS && \
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
COPY --from=artifacts --chown=$USER:$GROUP /release/fe_artifacts/ $STARROCKS_ROOT/

# Copy fe k8s scripts to the runtime container image
COPY --chown=$USER:$GROUP docker/dockerfiles/fe/*.sh $STARROCKS_ROOT/

# Create directory for FE metadata
RUN mkdir -p $STARROCKS_ROOT/fe/meta

ENTRYPOINT ["/usr/bin/tini-static", "--"]


FROM base_image AS runas_minimal_true
# Nothing to do, the USER is set to $USER in base_image


FROM base_image AS runas_minimal_false
ARG RUN_AS_USER
USER $RUN_AS_USER


FROM runas_minimal_${MINIMAL}
