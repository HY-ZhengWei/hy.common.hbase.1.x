package org.hy.common.hbase.junit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.hy.common.CounterMap;
import org.hy.common.Date;
import org.hy.common.JavaHelp;
import org.hy.common.hbase.HBase;
import org.hy.common.hbase.HBase.ResultType;





/**
 * 管理功能的测试
 * 
 * @author ZhengWei(HY)
 * @create 2014-05-28
 */
public class JU_HBase_Admin
{
    
    public static void main(String [] args)
    {
        HBase        v_HBase      = new HBase("192.168.1.123");
        List<String> v_TableNames = v_HBase.getTableNames();
        
        
        v_HBase.setResultType(ResultType.ResultType_HData);
        
        
        // 获取数据库所有表名称
        for (int i=0; i<v_TableNames.size(); i++)
        {
            System.out.println("-- " + (i + 1) + " : " + v_TableNames.get(i));
            
            // 获取表的列族信息
            List<String> v_Familys = v_HBase.getTableFamilyNames(v_TableNames.get(i));
            
            for (int x=0; x<v_Familys.size(); x++)
            {
                System.out.println("-- \t" + (x + 1) + " : " + v_Familys.get(x));
            }
            
            System.out.println();
        }
        
        
        // 创建表、添加列族，删除列族，删除表
        String v_TableName = "HY" + Date.getNowTime().getYMD_ID();
        v_HBase.createTable(v_TableName ,"CF01");
        v_HBase.addFamily(v_TableName ,"CF02");
        v_HBase.dropFamily(v_TableName ,"CF02");
        v_HBase.dropTable(v_TableName);
        
        
        
        // 获取数据库所有表名称
        for (int i=0; i<v_TableNames.size(); i++)
        {
            System.out.println("-- " + (i + 1) + " : " + v_TableNames.get(i));
            
            // 获取表的结构信息
            CounterMap<String> v_Structure = v_HBase.getTableStructure(v_TableNames.get(i));
            
            if ( !JavaHelp.isNull(v_Structure) )
            {
                int v_ColIndex = 0;
                List<String> v_ColNames = new ArrayList<String>(v_Structure.keySet());
                Collections.sort(v_ColNames);
                for (String v_ColName : v_ColNames)
                {
                    System.out.println("-- \t" + (++v_ColIndex) + " : " + v_ColName + "\t" + v_Structure.get(v_ColName));
                }
            }
            
            System.out.println();
        }
    }
    
}
