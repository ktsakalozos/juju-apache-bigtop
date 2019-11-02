#!/bin/sh

set -ex

# Verify required directories exist
if [ ! -d "${JUJU_REPOSITORY}" ]; then
  echo "JUJU_REPOSITORY must be set to the base charm directory. Bailing out."
  exit 1
fi
BIGTOP_BUNDLE_SRC="${JUJU_REPOSITORY}/bigtop/bigtop-deploy/juju"
if [ ! -d "${BIGTOP_BUNDLE_SRC}" ]; then
  echo "Could not find bundle source in ${BIGTOP_BUNDLE_SRC}. Bailing out."
  exit 1
fi

# Handle script args
if [ -z "$1" ]; then
  RELEASE_OPTS="--channel edge"
else
  RELEASE_OPTS="--channel $1"
fi

# Setup log
RUN=`date +%s`
RESULT_LOG="/tmp/bigtop-release-bundles-${RUN}.log"

echo 'Releasing Bigtop bundles to bigdata-charmers'
cd ${BIGTOP_BUNDLE_SRC}
for i in hadoop-hbase hadoop-kafka hadoop-processing hadoop-spark spark-processing; do
  echo "doing $i"
  cd $i
  charm proof || exit 1
  url=`charm push . cs:~bigdata-charmers/bundle/$i | grep url | awk '{print $2}'`
  echo "releasing $url"
  charm release $url ${RELEASE_OPTS} | tee -a ${RESULT_LOG}
  echo "setting homepage bugs-url for $i"
  charm set cs:~bigdata-charmers/$i \
    homepage=https://github.com/apache/bigtop/tree/master/bigtop-deploy/juju \
    bugs-url=https://github.com/juju-solutions/bigtop/issues \
    ${RELEASE_OPTS}
  echo "granting perms on $i"
  charm grant cs:~bigdata-charmers/bundle/$i everyone ${RELEASE_OPTS}
  cd -
done
cd
