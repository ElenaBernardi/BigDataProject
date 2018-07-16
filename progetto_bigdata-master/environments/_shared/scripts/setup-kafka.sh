#!/bin/bash

source "/home/bigData/_shared/scripts/common.sh"



KAFKA_VERSION=1.0.1
KAFKA_ARCHIVE=kafka_2.11-${KAFKA_VERSION}.tgz
GET_KAFKA_URL=http://mirror.cogentco.com/pub/apache/kafka/${KAFKA_VERSION}/kafka_2.11-${KAFKA_VERSION}.tgz
KAFKA_PATH=/usr/local/kafka 


function installLocalKafka {
	echo "================"
	echo "installing kafka"
	echo "================"
	FILE=${BIGDATA_DOWNLOADS}/kafka_2.11-${KAFKA_VERSION}.tgz
	tar -xzf $FILE -C /usr/local
	mv /usr/local/kafka_2.11-${KAFKA_VERSION} /usr/local/kafka
}

function installRemoteKafka {
	echo "================="
	echo "downloading kafka"
	echo "================="
	wget -q -P ${BIGDATA_DOWNLOADS} ${GET_KAFKA_URL}
	installLocalKafka
}



function installKafka {
	if downloadExists $KAFKA_ARCHIVE; then
		installLocalKafka
	else
		installRemoteKafka
	fi
}

function installPrerequisites {
	echo "installing unzip"
	apt-get install unzip 
}

echo "---setup kafka---"
installPrerequisites
installKafka

