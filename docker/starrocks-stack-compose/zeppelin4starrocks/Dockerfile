FROM apache/zeppelin:0.10.1

# update interpreter config(default to jdbc:mysql) before starting the service
ADD interpreter.json /opt/zeppelin/conf/

CMD ["bin/zeppelin.sh"]
