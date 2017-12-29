

cd .\bin

rd /s/q .\org\hy\common\hbase\junit

jar cvfm hy.common.hbase.jar MANIFEST.MF META-INF org

copy hy.common.hbase.jar ..
del /q hy.common.hbase.jar
cd ..

