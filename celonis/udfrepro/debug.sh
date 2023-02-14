log_stdin()
{
    echo "[`date`] $@" >&1
}

# TODO: switch the image for A/B test
# udffailure-main-76c7249b is image built from main branch and it has no issue,
# udffailure-branch2.5-facfd5a0 is imaged built from branch2.5-facfd5a0, and has UDF flaky error.
# ubuntu_image="ghcr.io/dengliu/starrocks-allin1:udffailure-main-76c7249b"
ubuntu_image="ghcr.io/dengliu/starrocks-allin1:udffailure-branch2.5-facfd5a0"


for VARIABLE in {1..100}
do
  log_stdin Loop: $VARIABLE "============================="

  docker run -it --name starrocks_allin1_reproduce --env STARTMODE=auto -p 8030:8030 -p 8040:8040 -p 9030:9030 -d $ubuntu_image

#  docker cp ../../java-extensions/udffailures/target/udf-failure-reproduce-1.0-SNAPSHOT-jar-with-dependencies.jar  starrocks_allin1_reproduce:/data/deploy/starrocks/udf/udf-failure-reproduce-1.0-SNAPSHOT.jar

  sleep 30
  mysql -uroot -h 127.0.0.1 -P9030 < debug.sql

  ## Do clean up staff.
  docker rm -f starrocks_allin1_reproduce
done
