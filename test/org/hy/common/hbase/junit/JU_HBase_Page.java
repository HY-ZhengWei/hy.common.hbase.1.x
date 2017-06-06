package org.hy.common.hbase.junit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hy.common.hbase.HData;
import org.hy.common.hbase.HPage;
import org.hy.common.hbase.HBase;





/**
 * 测试分页功能
 * 
 * @author ZhengWei(HY)
 * @create 2014-05-22
 */
public class JU_HBase_Page
{

    public static void main(String [] args)
    {
        HBase                            v_HBase  = new HBase("192.168.105.107");
        HPage                            v_HPage  = new HPage(10);
        Map<String ,Map<String ,Object>> v_Rows   = null;
        int                              v_Count  = 0;
        String                           v_Table  = "item";
        List<HData>                      v_HDatas = new ArrayList<HData>();
        
        v_HBase.setColOrder(true);
        
        
        v_HDatas.add(new HData("" ,"examStatus" ,"0")       .setFamilyName("itemCF"));
        v_HDatas.add(new HData("" ,"itemType"   ,"product0").setFamilyName("itemCF"));
        // 上下两种条件是不同的
//        v_HDatas.add(new HData("" ,"examStatus" ,"0")       );
//        v_HDatas.add(new HData("" ,"itemType"   ,"product0"));
        
        v_Rows = v_HBase.getRows(v_Table ,v_HPage ,v_HDatas);
        
        while ( v_HPage.getRowKey() != null )
        {
            for (String v_RowKey : v_Rows.keySet())
            {
                System.out.println("-- " + (++v_Count) + " : " + v_RowKey + " = " + v_Rows.get(v_RowKey));
            }
            
            v_Rows = v_HBase.getRows(v_Table ,v_HPage ,v_HDatas);
        }
        
        
        for (String v_RowKey : v_Rows.keySet())
        {
            System.out.println("-- " + (++v_Count) + " : " + v_RowKey + " = " + v_Rows.get(v_RowKey));
        }
        
        
        System.out.println("-- Row Count = " + v_HBase.getCount(v_Table ,v_HDatas));
    }
    
}
