package org.hy.common.hbase.junit;

import java.util.ArrayList;
import java.util.List;

import org.hy.common.StringHelp;
import org.hy.common.hbase.HBase;
import org.hy.common.hbase.HData;





/**
 * 插入及更新功能的测试
 * 
 * @author ZhengWei(HY)
 * @create 2014-05-27
 */
public class JU_HBase_Put
{
    
    public static void main(String [] args)
    {
        HBase       v_HBase  = new HBase("192.168.105.107");
        List<HData> v_HDatas = new ArrayList<HData>();
        
        
        // 更新一个字段(先删除，后插入)
        // v_HBase.update("item" ,"item_2014051608550005" ,"itemCF" ,"isExit" ,"1");
        
        
        
        for (int v_RowIndex=1; v_RowIndex<=3; v_RowIndex++)
        {
            String v_RowKey = "R" + StringHelp.lpad(v_RowIndex ,3 ,"0");
            
            for (int v_ColumnIndex=1; v_ColumnIndex<=10; v_ColumnIndex++)
            {
                v_HDatas.add(new HData(v_RowKey ,"C" + StringHelp.lpad(v_ColumnIndex ,3 ,"0") ,"R" + v_RowIndex + ",C" + v_ColumnIndex).setFamilyName("CF"));
            }
        }
        
        v_HBase.update("HY20140527" ,v_HDatas);
        // v_HBase.update("HY20140527" ,v_HDatas);
    }
    
}
