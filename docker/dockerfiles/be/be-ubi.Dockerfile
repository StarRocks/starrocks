# This docker file build the Starrocks be ubi8 image
# NOTE: temporary solution, use centos7 artifacts to deliver fe/be/cn/allin1 image
#
# Please run this command from the git repo root directory to build:
#   - Use artifact image to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=image --build-arg ARTIFACTIMAGE=ghcr.io/starrocks/starrocks/artifact-centos7:latest -f docker/dockerfiles/be/be-ubi.Dockerfile -t be-ubi:latest .
#   - Use locally build artifacts to package runtime container:
#     > DOCKER_BUILDKIT=1 docker build --build-arg ARTIFACT_SOURCE=local --build-arg LOCAL_REPO_PATH=. -f docker/dockerfiles/be/be-ubi.Dockerfile -t be-ubi:latest .
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
COPY ${LOCAL_REPO_PATH}/output/be /release/be_artifacts/be


FROM artifacts-from-${ARTIFACT_SOURCE} as artifacts


FROM registry.access.redhat.com/ubi8/ubi:8.7
ARG STARROCKS_ROOT=/opt/starrocks

RUN yum install -y java-1.8.0-openjdk-devel tzdata openssl curl vim ca-certificates fontconfig gzip tar less hostname procps-ng lsof && \
    rpm -ivh https://repo.mysql.com/mysql57-community-release-el7.rpm && \
    yum -y install mysql-community-client --nogpgcheck && \
    yum remove -y mysql57-community-release-el7
ENV JAVA_HOME=/usr/lib/jvm/java-openjdk

RUN touch /.dockerenv

WORKDIR $STARROCKS_ROOT

# Run as starrocks user
ARG USER=starrocks
ARG GROUP=starrocks
RUN groupadd --gid 1000 $GROUP && useradd --no-create-home --uid 1000 --gid 1000 \
             --shell /usr/sbin/nologin $USER && \
    chown -R $USER:$GROUP $STARROCKS_ROOT
USER $USER

# Copy all artifacts to the runtime container image
COPY --from=artifacts --chown=starrocks:starrocks /release/be_artifacts/ $STARROCKS_ROOT/

# Copy be k8s scripts to the runtime container image
COPY --chown=starrocks:starrocks docker/dockerfiles/be/*.sh $STARROCKS_ROOT/

# Create directory for BE storage, create cn symbolic link to be
RUN mkdir -p $STARROCKS_ROOT/be/storage && ln -sfT be $STARROCKS_ROOT/cn

# run as root by default
USER root
