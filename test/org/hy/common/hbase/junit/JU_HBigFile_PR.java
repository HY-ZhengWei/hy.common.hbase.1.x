package org.hy.common.hbase.junit;

import java.io.File;
import java.io.IOException;

import org.hy.common.Date;
import org.hy.common.StringHelp;
import org.hy.common.hbase.plugins.HBigFile;
import org.hy.common.xml.XJava;
import org.junit.Test;





/**
 * 压力测试类：用HBase数据库保存大文件的相关操作  
 *
 * @author      ZhengWei(HY)
 * @createDate  2015-03-20
 * @version     v1.0
 */
public class JU_HBigFile_PR
{
    
    private static boolean $Init = false;
    
    
    
    public static void main(String [] args) throws Exception
    {
        new JU_HBigFile_PR().pr_Write();
    }
    
    
    
    public JU_HBigFile_PR() throws Exception
    {
        if ( !$Init )
        {
            $Init = true;
            XJava.parserAnnotation(JU_HBigFile.class.getName());
        }
    }
    
    
    
    /**
     * 向HBase数据库写入文件内容的压力测试
     * 
     * @author      ZhengWei(HY)
     * @createDate  2015-03-20
     * @version     v1.0
     *
     * @throws IOException
     */
    public void pr_Write() throws IOException
    {
        HBigFile        v_HBigFile = (HBigFile)XJava.getObject("HBigFile");
        File            v_File     = new File("/Users/hy/Pictures/e995981.jpg");
        String          v_FileID   = null;
        
        for (int i=1; i<=100000; i++)
        {
            Date v_BeginTime = Date.getNowTime();
            System.out.println("-- " + v_BeginTime.getFullMilli() + " 开始写数据入库(" + StringHelp.lpad(i ,5 ,"0") + ") ... ...");
            
            // 保存到数据库中
            v_FileID = v_HBigFile.write(v_File);
            
            Date v_EndTime = Date.getNowTime();
            System.out.println("-- " + v_EndTime.getFullMilli() + " 完成写数据入库。共用时 " + (v_EndTime.getTime() - v_BeginTime.getTime()) + " \tFileID=" + v_FileID);
        }
    }
    
    
    
    /**
     * 在数据库中写入大量文件信息后，测试读取文件的性能
     * 
     * @author      ZhengWei(HY)
     * @createDate  2015-03-20
     * @version     v1.0
     *
     * @throws IOException
     */
    @Test
    public void test_001_Read() throws IOException
    {
        HBigFile v_HBigFile = (HBigFile)XJava.getObject("HBigFile");
        
        Date v_BeginTime = Date.getNowTime();
        System.out.println("-- " + v_BeginTime.getFullMilli() + " 开始读数据到本地文件 ... ...");
        
        v_HBigFile.readToFile("201503201105588150424" ,"/Users/hy/Downloads/数据库第一条记录.jpg" ,true);
        v_HBigFile.readToFile("201503201229575630690" ,"/Users/hy/Downloads/数据库中间部位的.jpg" ,true);
        v_HBigFile.readToFile("201503201408210090944" ,"/Users/hy/Downloads/数据库最后的记录.jpg" ,true);
        
        Date v_EndTime = Date.getNowTime();
        System.out.println("-- " + v_EndTime.getFullMilli() + " 完成读数据到本地文件。共用时 " + (v_EndTime.getTime() - v_BeginTime.getTime()));
    }
    
}
