#!/bin/bash

source "/home/bigData/_shared/scripts/common.sh"

HBASE_VERSION=2.0.1
HBASE_ARCHIVE=hbase-${HBASE_VERSION}-bin.tar.gz
GET_HBASE_URL=http://it.apache.contactlab.it/hbase/2.0.1/hbase-2.0.1-bin.tar.gz
HBASE_PATH=/url/local/hbase

function installHbase {
	if downloadExists $HBASE_ARCHIVE; then
		installLocalHbase
	else
		installRemoteHbase
	fi
}

function installLocalHbase {
	echo "================"
	echo "installing hbase"
	echo "================"
	FILE=${BIGDATA_DOWNLOADS}/${HBASE_ARCHIVE}
	sudo tar -xzf $FILE -C /usr/local
	FILE=hbase-${HBASE_VERSION}
	sudo mv /usr/local/$FILE /usr/local/hbase
	sudo chmod 777 -R /usr/local/hbase/
}

function installRemoteHbase {
	echo "================="
	echo "downloading hbase"
	echo "================="
	wget -q -P ${BIGDATA_DOWNLOADS} ${GET_HBASE_URL}
	installLocalHbase
}

function setupEnvVars {
	echo "creating hbase environment variables"
	echo export HBASE_HOME=/usr/local/hbase >> ~/.bashrc
	echo export PATH=\${PATH}:\${HBASE_HOME}/bin >> ~/.bashrc
}

function editingFiles {
	echo "editing files"
	sudo cp ${BIGDATA_FILES}/hbase-env.sh /usr/local/hbase/conf/hbase-env.sh
	sudo cp ${BIGDATA_FILES}/hbase-site.xml /usr/local/hbase/conf/hbase-site.xml 
}

echo "---setup hbase---"
installHbase
setupEnvVars
editingFiles