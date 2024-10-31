#/bin/bash
set -e -o pipefail

basedir=$(cd $(dirname ${BASH_SOURCE:-$0}); pwd)
cd ${basedir}

./build.sh --clean --fe

mvn install:install-file \
    -Dfile=fe/spark-dpp/target/spark-dpp-1.0.0.jar \
    -DgroupId=com.starrocks \
    -DartifactId=spark-dpp \
    -Dversion=1.0.0 \
    -Dpackaging=jar


pushd fe/fe-core

sed -e '/^\s*<finalName>starrocks-fe<\/finalName>\s*$/{n; /^\s*<plugins>\s*$/a\
          <plugin>\
            <groupId>org.apache.maven.plugins</groupId>\
            <artifactId>maven-assembly-plugin</artifactId>\
            <version>3.3.0</version>\
            <configuration>\
              <descriptorRefs>\
                <descriptorRef>jar-with-dependencies</descriptorRef>\
              </descriptorRefs>\
              <appendAssemblyId>false</appendAssemblyId>\
            </configuration>\
            <executions>\
              <execution>\
                <id>make-assembly</id>\
                <phase>package</phase>\
                <goals>\
                  <goal>single</goal>\
                </goals>\
                <configuration>\
                  <finalName>fe-core-jar-with-dependencies-${project.version}</finalName>\
                  <includeTestClasses>true</includeTestClasses>\
                  <includeDependencies>true</includeDependencies>\
                </configuration>\
              </execution>\
            </executions>\
          </plugin>
        }' pom.xml > assembly-pom.xml

version=$(mvn help:evaluate -Dexpression=project.version -q -DforceStdout)
PROGUARD=${basedir}/build-support/proguard-java11.cfg
PYTHON=python3 mvn package -DskipTests=true -Dproguard-cfg=${PROGUARD} -f assembly-pom.xml
rm assembly-pom.xml
mvn install:install-file \
    -Dfile=target/fe-core-jar-with-dependencies-${version}.jar \
    -DgroupId=com.starrocks \
    -DartifactId=fe-core-jar-with-dependencies \
    -Dversion=${version} \
    -Dpackaging=jar
popd

pushd fe
sed "s/AUTOMV_RECOMMENDER_VERSION/${version}/" pom-automv-recommender.xml > pom-automv-recommender-current.xml
mvn package -DskipTests=true -f pom-automv-recommender-current.xml
rm pom-automv-recommender-current.xml

[ -d automv_recommender ] && rm -fr automv_recommender
mkdir automv_recommender
cp target/automv_recommender-${version}.jar automv_recommender/
cat >automv_recommender/automv_recommender <<DONE
#!/bin/bash
set -e -o pipefail
basedir=\$(cd \$(dirname \${BASH_SOURCE:-\$0});pwd)
echo \$basedir
java -javaagent:\${basedir}/jmockit-1.49.4.jar -cp \${basedir}/automv_recommender-${version}.jar com.starrocks.sql.automv.lattice.QueryDumpMVRecommenderCmd \$*
DONE
chmod a+x automv_recommender/automv_recommender
cp  -r fe-core/src/test/resources/sql/query_dump/automv automv_recommender/queryDump
mvnRepo=$(mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout)
cp ${mvnRepo}/com/github/hazendaz/jmockit/jmockit/1.49.4/jmockit-1.49.4.jar automv_recommender/

cd automv_recommender
if (./automv_recommender -h |grep "usage: recommend MV from query dump");then
  echo "Fail to build AutoMVRecommender"
  exit 1
else
  echo "Build AutoMVRecommender successfully"
fi

cd ../
tar czvf automv_recommender.tgz automv_recommender
rm -fr automv_recommender
