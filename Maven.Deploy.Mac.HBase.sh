#!/bin/sh

mvn deploy:deploy-file -Dfile=hy.common.hbase.jar                              -DpomFile=./src/META-INF/maven/org/hy/common/hbase/pom.xml -DrepositoryId=thirdparty -Durl=http://HY-ZhengWei:1481/repository/thirdparty
mvn deploy:deploy-file -Dfile=hy.common.hbase-sources.jar -Dclassifier=sources -DpomFile=./src/META-INF/maven/org/hy/common/hbase/pom.xml -DrepositoryId=thirdparty -Durl=http://HY-ZhengWei:1481/repository/thirdparty
