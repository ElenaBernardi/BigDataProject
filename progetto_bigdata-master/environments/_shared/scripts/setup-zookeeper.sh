#!/bin/bash

source "/home/bigData/_shared/scripts/common.sh"



ZOOKEEPER_VERSION=3.4.10
ZOOKEEPER_ARCHIVE=zookeeper-${ZOOKEEPER_VERSION}.tar.gz
GET_ZOOKEEPER_URL=http://it.apache.contactlab.it/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/zookeeper-${ZOOKEEPER_VERSION}.tar.gz
ZOOKEEPER_PATH=/usr/local/zookeeper


function installLocalZookeeper {
	echo "===================="
	echo "installing zookeeper"
	echo "===================="
	FILE=${BIGDATA_DOWNLOADS}/zookeeper-${ZOOKEEPER_VERSION}.tar.gz
	tar -xzf $FILE -C /usr/local
	mv /usr/local/zookeeper-${ZOOKEEPER_VERSION} /usr/local/zookeeper
	cd /usr/local/zookeeper/conf/
	echo $'tickTime=2000\ndataDir=/var/zookeeper\nclientPort=2181' >zoo.cfg

}

function installRemoteZookeeper {
	echo "====================="
	echo "downloading zookeeper"
	echo "====================="
	wget -q -P ${BIGDATA_DOWNLOADS} ${GET_ZOOKEEPER_URL}
	installLocalZookeeper
}



function installZookeeper {
	if downloadExists $ZOOKEEPER_ARCHIVE; then
		installLocalZookeeper
	else
		installRemoteZookeeper
	fi
}

function installPrerequisites {
	echo "installing unzip"
	apt-get install unzip 
}

echo "---setup zookeeper---"
installPrerequisites
installZookeeper

