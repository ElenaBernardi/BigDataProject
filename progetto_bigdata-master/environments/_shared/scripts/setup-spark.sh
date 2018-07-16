#!/bin/bash

source "/home/bigData/_shared/scripts/common.sh"

SPARK_VERSION=2.1.0
SPARK_ARCHIVE=spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
GET_SPARK_URL=https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop2.7.tgz

function installRemoteSpark {
	echo "================="
	echo "downloading spark"
	echo "================="
	wget -q -P ${BIGDATA_DOWNLOADS} ${GET_SPARK_URL}
	installLocalSpark
}

function installLocalSpark {
	echo "================="
	echo "installing spark"
	echo "================="
	FILE=${BIGDATA_DOWNLOADS}/${SPARK_ARCHIVE}
	sudo tar -xzf $FILE -C /usr/local
	FILE=spark-${SPARK_VERSION}-bin-hadoop2.7
	sudo mv /usr/local/$FILE /usr/local/spark
}

function installSpark {
	if downloadExists $SPARK_ARCHIVE; then
		installLocalSpark
	else
		installRemoteSpark
	fi
}

function setupEnvVars {
	echo "creating spark environment variables"
	echo export SPARK_HOME=/usr/local/spark >> ~/.bashrc
	echo export PATH=\${PATH}:\${SPARK_HOME}/bin >> ~/.bashrc
}

echo "---setup Spark---"
installSpark
setupEnvVars