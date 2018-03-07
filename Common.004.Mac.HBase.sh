#!/bin/sh

cd ./bin


rm -R ./org/hy/common/hbase/junit


jar cvfm hy.common.hbase.jar MANIFEST.MF META-INF org

cp hy.common.hbase.jar ..
rm hy.common.hbase.jar
cd ..





cd ./src
jar cvfm hy.common.hbase-sources.jar MANIFEST.MF META-INF org 
cp hy.common.hbase-sources.jar ..
rm hy.common.hbase-sources.jar
cd ..
