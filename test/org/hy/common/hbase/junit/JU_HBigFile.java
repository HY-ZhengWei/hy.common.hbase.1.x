package org.hy.common.hbase.junit;

import java.io.File;

import org.hy.common.Date;
import org.hy.common.hbase.plugins.HBigFile;
import org.hy.common.xml.XJava;
import org.hy.common.xml.annotation.XType;
import org.hy.common.xml.annotation.Xjava;





/**
 * 测试类：用HBase数据库保存大文件的相关操作  
 *
 * @author      ZhengWei(HY)
 * @createDate  2015-03-14
 * @version     v1.0
 */
@Xjava(XType.XML)
public class JU_HBigFile
{
    
    public static void main(String [] args) throws Exception
    {
        XJava.parserAnnotation(JU_HBigFile.class.getName());
        
        HBigFile        v_HBigFile = (HBigFile)XJava.getObject("HBigFile");
        File            v_File     = new File("/Users/hy/Pictures/e995981.jpg");
        String          v_FileID   = null;
        
        
        Date v_BeginTime = Date.getNowTime();
        System.out.println("-- " + v_BeginTime.getFullMilli() + " 开始写数据入库 ... ...");
        
        // 保存到数据库中
        v_FileID = v_HBigFile.write(v_File);
        
        Date v_EndTime = Date.getNowTime();
        System.out.println("-- " + v_EndTime.getFullMilli() + " 完成写数据入库。共用时 " + (v_EndTime.getTime() - v_BeginTime.getTime()));
        
        // 从数据库中读出
        v_HBigFile.readToFile(v_FileID ,"/Users/hy/Pictures/" ,"ReadToFile.rmvb" ,true);
        
        Date v_EndTime2 = Date.getNowTime();
        System.out.println("-- " + v_EndTime2.getFullMilli() + " 完成读数据到本地文件。共用时 " + (v_EndTime2.getTime() - v_EndTime.getTime()));
    }
    
}
