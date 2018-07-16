#!/bin/bash

# dove vengono montate le risorse e i download condivisi 
BIGDATA_HOME=/home/bigData
BIGDATA_DOWNLOADS=${BIGDATA_HOME}/_shared/downloads
BIGDATA_RESOURCES=${BIGDATA_HOME}/_shared/resources
BIGDATA_FILES=${BIGDATA_HOME}/_shared/files

function resourceExists {
	FILE=${BIGDATA_RESOURCES}/$1
	if [ -e $FILE ]
	then
		return 0
	else
		return 1
	fi
}

function downloadExists {
	FILE=${BIGDATA_DOWNLOADS}/$1
	if [ -e $FILE ]
	then
		return 0
	else
		return 1
	fi
}

function fileExists {
	FILE=$1
	if [ -e $FILE ]
	then
		return 0
	else
		return 1
	fi
}

#echo "common loaded"
