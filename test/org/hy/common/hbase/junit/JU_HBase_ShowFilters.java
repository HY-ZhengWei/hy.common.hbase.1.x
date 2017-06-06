package org.hy.common.hbase.junit;

import java.util.Map;

import org.hy.common.hbase.HBase;
import org.hy.common.hbase.HData;





public class JU_HBase_ShowFilters
{
    
    public static void main(String [] args)
    {
        HBase v_HBase = new HBase("192.168.105.107");
        
        Map<String ,Map<String ,Object>> v_Maps = v_HBase.getRows("item" 
                                                                 ,new HData("" ,"itemType" ,"product0")
                                                                 ,new HData("" ,"examStatus" ,"1")
                                                                 );
        
        int v_RowNo = 0;
        for (String v_RowKey : v_Maps.keySet())
        {
            Map<String ,Object> v_Row = v_Maps.get(v_RowKey);
            
            ++v_RowNo;
            
            for (String v_ColumnName : v_Row.keySet())
            {
                System.out.println("-- " + v_RowNo + " " + v_RowKey + "\t" + v_ColumnName + " = " + v_Row.get(v_ColumnName));
            }
            
            System.out.println();
        }
        
        
        System.out.println("-- RCount = " + v_HBase.getCount("item" ,new HData("" ,"itemType" ,"product0") ,new HData("" ,"examStatus" ,"1")));
        
        System.out.println("-- Row Count = " + v_HBase.getCount("item" ,(Map<String ,Object>)null));
    }
    
}
