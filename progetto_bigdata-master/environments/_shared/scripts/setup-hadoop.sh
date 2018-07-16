#!/bin/bash

source "/home/bigData/_shared/scripts/common.sh"

HADOOP_VERSION=2.7.6
HADOOP_ARCHIVE=hadoop-${HADOOP_VERSION}.tar.gz
GET_HADOOP_URL=http://mirror.nohup.it/apache/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz
HADOOP_PATH=/url/local/hadoop

function installRemoteHadoop {
	echo "================="
	echo "downloading hadoop"
	echo "================="
	wget -q -P ${BIGDATA_DOWNLOADS} ${GET_HADOOP_URL}
	installLocalHadoop
}

function installLocalHadoop {
	echo "================"
	echo "installing hadoop"
	echo "================"
	FILE=${BIGDATA_DOWNLOADS}/hadoop-${HADOOP_VERSION}.tar.gz
	sudo tar -xzf $FILE -C /usr/local
	FILE=hadoop-${HADOOP_VERSION}
	sudo mv /usr/local/$FILE /usr/local/hadoop
	sudo chmod 777 -R /usr/local/hadoop/
}

function installHadoop {
	if downloadExists $HADOOP_ARCHIVE; then
		installLocalHadoop
	else
		installRemoteHadoop
	fi
}

function setupEnvVars {
	echo "creating hadoop environment variables"
	echo export JAVA_HOME=/usr/local/java >> ~/.bashrc
	echo export PATH=\${JAVA_HOME}/bin:\${PATH} >> ~/.bashrc
	echo export HADOOP_HOME=/usr/local/hadoop >> ~/.bashrc
	echo export PATH=\${PATH}:\${HADOOP_HOME}/bin >> ~/.bashrc
}

function editingFiles {
	echo "editing files"
	sudo cp ${BIGDATA_FILES}/hadoop-env.sh /usr/local/hadoop/etc/hadoop/hadoop-env.sh
	sudo cp ${BIGDATA_FILES}/hdfs-site.xml /usr/local/hadoop/etc/hadoop/hdfs-site.xml 
	sudo cp ${BIGDATA_FILES}/core-site.xml /usr/local/hadoop/etc/hadoop/core-site.xml
	sudo mv /usr/local/hadoop/etc/hadoop/mapred-site.xml.template /usr/local/hadoop/etc/hadoop/mapred-site.xml
	sudo cp ${BIGDATA_FILES}/mapred-site.xml /usr/local/hadoop/etc/hadoop/mapred-site.xml
}

function prerequisites {
	echo "prerequisites"
	sudo mkdir -p /app/hadoop/tmp/
	sudo chmod 777 -R /app/hadoop/tmp
}


function format {
	echo "format"
	sudo /usr/local/hadoop/bin/hadoop namenode -format
	sudo chmod 777 -R /app/hadoop/tmp/name
}

echo "---setup hadoop---"
prerequisites
installHadoop
setupEnvVars
editingFiles
format