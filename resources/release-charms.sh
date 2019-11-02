#!/bin/sh

set -ex

# Verify required directories exist
if [ ! -d "${JUJU_REPOSITORY}" ]; then
  echo "JUJU_REPOSITORY must be set to the base charm directory. Bailing out."
  exit 1
fi

# Handle script args
if [ -z "$1" ]; then
  RELEASE_OPTS="--channel edge"
else
  RELEASE_OPTS="--channel $1"
fi

# Series is only used to find the built charm (we build with -s xenial
# because bigtop repos have xenial packages).
SERIES="xenial"

# Manage the bigtop repo resource
BIGTOP_REPO_RELEASE="bigtop-repo-8"
# NB: UNCOMMENT IF YOU WANT TO ADD A NEW RESOURCE
#RESOURCE="/tmp/branch-1.4.zip"
#test -f ${RESOURCE} && rm -f ${RESOURCE}
#sudo apt install wget
#wget https://github.com/apache/bigtop/archive/branch-1.4.zip -O ${RESOURCE}

# Setup log
RUN=`date +%s`
RESULT_LOG="/tmp/bigtop-release-charms-${RUN}.log"

echo 'Releasing Bigtop charms to bigdata-charmers'
cd ${JUJU_REPOSITORY}/${SERIES}
for i in hadoop-namenode hadoop-plugin hadoop-resourcemanager hadoop-slave hbase hive kafka mahout spark zeppelin zookeeper; do
  echo "doing $i"
  cd $i
  charm proof || exit 1
  url=`charm push . cs:~bigdata-charmers/$i ${RESOURCE:+--resource bigtop-repo=$RESOURCE} | grep url | awk '{print $2}'`
  echo "releasing $url"
  if [ $i = 'spark' ]; then
    charm release $url --resource ${BIGTOP_REPO_RELEASE} --resource sample-data-1 ${RELEASE_OPTS} | tee -a ${RESULT_LOG}
  else
    charm release $url --resource ${BIGTOP_REPO_RELEASE} ${RELEASE_OPTS} | tee -a ${RESULT_LOG}
  fi
  echo "setting homepage bugs-url for $i"
  charm set cs:~bigdata-charmers/$i \
    homepage=https://github.com/apache/bigtop/tree/master/bigtop-packages/src/charm \
    bugs-url=https://github.com/juju-solutions/bigtop/issues \
    ${RELEASE_OPTS}
  echo "granting perms on $i"
  charm grant cs:~bigdata-charmers/$i everyone ${RELEASE_OPTS}
  cd -
done
cd
