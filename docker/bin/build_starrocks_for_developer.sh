# This script is to build starrocks in a docker enviroment for developer
# Attention: user should have sudo permission !!
# user_name is linux's user name, for example xyz
# user_root_path is user's root path, for example /home/xyz
# starrocks_path is starrocks path, for example /home/xyz/starrocks
# docker_container is the container name in docker, for example xyz-docker

user_name=xxx
user_root_path=xxx
starrocks_path=xxx
docker_container_name=xxx

# 1. clean old custome_env.sh 
mv ${starrocks_path}/custom_env.sh ${starrocks_path}/custom_env_copyp.sh
touch ${starrocks_path}/custom_env.sh

# 2. pull image
sudo docker pull starrocks/dev-env:main

# 3. create container, remove the dockere container if it exist
sudo docker stop $(sudo docker ps -a | grep "${docker_container_name}" | awk '{print $1 }')
sudo docker rm $(sudo docker ps -a | grep "${docker_container_name}" | awk '{print $1 }')
sudo docker run -it -v ${user_root_path}/.m2:/root/.m2  -v ${starrocks_path}:${starrocks_path} --name ${docker_container_name} -d starrocks/dev-env:main

# 4. build
sudo docker exec -it ${docker_container_name} ${starrocks_path}/build.sh
sudo chown -R ${user_name}:${user_name} ${starrocks_path}

# # 5. only build be
# sudo docker exec -it ${docker_container_name} ${starrocks_path}/build.sh --be
# sudo chown -R ${user_name}:${user_name} ${starrocks_path}

# # 5. only build fe
# sudo docker exec -it ${docker_container_name} ${starrocks_path}/build.sh --be
# sudo chown -R ${user_name}:${user_name} ${starrocks_path}