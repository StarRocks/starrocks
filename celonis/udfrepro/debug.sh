for VARIABLE in {1..100}
do
  echo Loop: $VARIABLE
  docker run -it --name starrocks_allin1_reproduce --env STARTMODE=auto -p 8030:8030 -p 8040:8040 -p 9030:9030 -d ghcr.io/celonis/celostar/starrocks-allin1-udf11:latest
  docker start starrocks_allin1_reproduce
  sleep 30
  mysql -uroot -h 127.0.0.1 -P9030 < debug.sql

  
  ## Do clean up staff.
  docker stop starrocks_allin1_reproduce
  docker rm starrocks_allin1_reproduce
done
