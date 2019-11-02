#!/bin/bash

set -ex

# Verify required directories exist
if [ ! -d "${JUJU_REPOSITORY}" ]; then
  echo "JUJU_REPOSITORY must be set to your base charm directory. Bailing out."
  exit 1
fi
BIGTOP_CHARM_SRC="${JUJU_REPOSITORY}/bigtop/bigtop-packages/src/charm"
if [ ! -d "${BIGTOP_CHARM_SRC}" ]; then
  echo "Could not find charm source in ${BIGTOP_CHARM_SRC}. Bailing out."
  exit 1
fi

# Handle script args
if [ "$1" = "local" ]; then
  BUILD_OPTS=""
else
  BUILD_OPTS="--no-local-layers"
fi

# Bigtop repo houses xenial packages; build charms for that series
SERIES="xenial"

# pipe separated list of charms we dont control
NO_CONTROL_PATTERN="giraph"

# Setup log
RUN=`date +%s`
RESULT_LOG="/tmp/bigtop-build-${RUN}.log"

echo 'Removing deps cache'
rm -rf ${JUJU_REPOSITORY}/deps/*

# Uncomment if you want to remove existing builds
#echo 'Removing bigtop charms'
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/hadoop-namenode
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/hadoop-plugin
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/hadoop-resourcemanager
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/hadoop-slave
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/hbase
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/hive
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/kafka
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/mahout
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/spark
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/zeppelin
#rm -rf ${JUJU_REPOSITORY}/${SERIES}/zookeeper

echo 'Building Bigtop layers'
cd ${BIGTOP_CHARM_SRC}
for i in `find * -type d -name 'layer-*' | grep -vE "${NO_CONTROL_PATTERN}"`; do
  (echo "doing $i"; cd $i; charm build -s ${SERIES} -r ${BUILD_OPTS}) | tee -a ${RESULT_LOG}
done
cd -
