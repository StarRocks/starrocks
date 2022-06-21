#!/usr/bin/bash

source ~/.bash_profile
set -eo pipefail

ROOT=`dirname "$0"`
ROOT=`cd "$ROOT/.."; pwd`

GITHUB_PR_NUMBER=${1:?"need GITHUB_PR_NUMBER parameter"}
GITHUB_PR_TARGET_BRANCH=${2:?"need GITHUB_PR_TARGET_BRANCH parameter"}

#GITHUB_PR_COMMENT_BODY=${3:-"default"}
echo "the github number"$GITHUB_PR_NUMBE
echo "the branch="$GITHUB_PR_TARGET_BRANCH
echo "the root path="$ROOT
echo "the env="$GITHUB_REF_NAME
# chown -R jenkins:jenkins $ROOT/../
ls -al

#git config  user.email "wanpengfei91@163.com"
#git config  user.name "wanpengfei-git"
#git stash
#git clean -df
#git checkout main
#for i in 1 2 3 4 5; do 
#    timeout 180 git pull && break || sleep 60
#done
#
#BRANCH_NAME="pr_${GITHUB_PR_NUMBER}_${RANDOM}"
#echo $BRANCH_NAME
#for i in 1 2 3 4 5; do 
#    timeout 180 git fetch origin pull/${GITHUB_PR_NUMBER}/head:${BRANCH_NAME} && break || sleep 60
#done
#git checkout ${BRANCH_NAME}
#git clean -df

#FE_FILE_CHANGE_STATUS=0
#git --no-pager diff --name-only FETCH_HEAD $(git merge-base FETCH_HEAD main) > files_change_list.txt
#while read line; do if [[ $line =~ ^fe/fe-core/* || $line =~ ^fe/spark-dpp/* || $line =~ ^fe/pom.xml || $line =~ ^run-fe-ut.sh || $line =~ ^build.sh || $line =~ ^gensrc/* ]];then FE_FILE_CHANGE_STATUS=1;break;fi;done < files_change_list.txt
#
#if (( $FE_FILE_CHANGE_STATUS == 0 ));then
#    exit 0
#fi

# branch like workgroup_main should also run main image
if [[ $GITHUB_PR_TARGET_BRANCH == *main* ]];then
    GITHUB_PR_TARGET_BRANCH="main"
fi

tmp_path=$(echo `cd "$ROOT/.."; pwd` | awk -F '/' '{print $NF}')
container_name=${GITHUB_PR_TARGET_BRANCH}_${tmp_path}
echo "===========container_name is $container_name=========="

docker stop $container_name || echo 1
docker rm $container_name || echo 1

echo "the docker map path="$ROOT
echo "the root ls="`ls /home/runner/work/starrocks/starrocks`
m2Path="/home/runner/"
docker run --privileged -v $m2Path/.m2:/root/.m2 -v $ROOT/starrocks:/root/starrocks -v /etc/timezone:/etc/timezone:ro -v /etc/localtime:/etc/localtime:ro --name $container_name -d starrocks/dev-env:main /bin/bash -c "while true;do echo hello;sleep 1;done"
sleep 10

echo "run docker for script"

cmd="cd /root/starrocks;
export FE_UT_PARALLEL=16;
timeout 3600 sh run-fe-ut.sh --run com.starrocks.utframe.Demo#testCreateDbAndTable+test2"

docker exec --privileged $container_name /bin/bash -c "$cmd"

echo "script run over-----"

