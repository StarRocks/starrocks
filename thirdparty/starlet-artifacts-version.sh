# Put this file into third_party/ so that whenever an update to STARLET dependency.
# starrocks/dev-env docker image will be triggered
# Indicate which starlet artifacts tag to be used
# * starrocks/starlet-artifacts-ubuntu22:${STARLET_ARTIFACTS_TAG}
# * starrocks/starlet-artifacts-centos7:${STARLET_ARTIFACTS_TAG}
#
# Check available tags at:
#   https://hub.docker.com/r/starrocks/starlet-artifacts-ubuntu22/tags
#   https://hub.docker.com/r/starrocks/starlet-artifacts-centos7/tags
#
# Update the following tag when STARLET releases a new version.
<<<<<<< HEAD
export STARLET_ARTIFACTS_TAG=v3.1.10
=======
export STARLET_ARTIFACTS_TAG=v3.4-rc1
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
