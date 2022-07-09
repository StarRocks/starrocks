#!/usr/bin/env bash

cd /home/disk2/liuxuefen/.m2/repository/com/starrocks
 
# echo 123456 | sudo -S cp -r /home/disk1/xupeng/.m2/repository/com/starrocks/starclient .
# echo 123456 | sudo -S cp -r /home/disk1/xupeng/.m2/repository/com/starrocks/staros .
# echo 123456 | sudo -S cp -r /home/disk1/xupeng/.m2/repository/com/starrocks/starproto .
# echo 123456 | sudo -S cp -r /home/disk1/xupeng/.m2/repository/com/starrocks/starmanager .

echo 123456 | sudo -S chgrp liuxuefen staros -R 
echo 123456 | sudo -S chown liuxuefen staros -R

echo 123456 | sudo -S chgrp liuxuefen starproto -R 
echo 123456 | sudo -S chown liuxuefen starproto -R

echo 123456 | sudo -S chgrp liuxuefen starclient -R 
echo 123456 | sudo -S chown liuxuefen starclient -R

echo 123456 | sudo -S chgrp liuxuefen starmanager -R 
echo 123456 | sudo -S chown liuxuefen starmanager -R