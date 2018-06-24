#!/usr/bin/env bash

echo "Switching branch"
git checkout jar-branch
git rebase master

echo "Building the Jar"
mvn clean compile assembly:single | grep "BUILD SUCCESS"
[[ $? == 0 ]] || ( echo "Failed to build" && exit 1 )

JAR_NAME="db-connector.jar"

echo "Copying the Jar locally"
cp target/${JAR_NAME} ./

git add -f ${JAR_NAME} && git commit -m "Adding the jar"
git push

echo "Returning to master"
git checkout master
