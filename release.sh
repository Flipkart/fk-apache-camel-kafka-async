#!/usr/bin/env bash

set -x
cat pom.xml | grep -F '<version>' | head -1 | sed -e 's/<[^/]*>//g' -e 's/<.*>//g' -e 's/ //g' | grep -q '\-t9$'

set -e
mvn clean install $*
#mvn deploy -q -DperformRelease=true -DskipTests -DaltDeploymentRepository=flipkart::default::http://artifactory.nm.flipkart.com:8081/artifactory/libs-release-local $*
mvn deploy -q -DperformRelease=true -DskipTests -DaltDeploymentRepository=flipkart::default::http://artifactory.nm.flipkart.com:8081/artifactory/libs-releases-local $*
#mvn deploy -q -DperformRelease=true -DskipTests -DaltDeploymentRepository=flipkart::default::http://artifactory.nm.flipkart.com:8081/artifactory/libs-snapshots-local

