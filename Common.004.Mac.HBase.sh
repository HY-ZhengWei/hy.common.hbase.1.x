#!/bin/sh

cd ./bin

rm -R ./org/hy/common/hbase/junit

jar cvfm hy.common.hbase.jar MANIFEST.MF LICENSE org

cp hy.common.hbase.jar ..
rm hy.common.hbase.jar
cd ..

