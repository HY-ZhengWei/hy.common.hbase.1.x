package org.hy.common.hbase.junit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.hy.common.hbase.HBase;
import org.hy.common.hbase.HData;





/**
 * 查询功能的测试
 * 
 * @author ZhengWei(HY)
 * @create 2014-05-26
 */
public class JU_HBase
{

    @SuppressWarnings("unused")
    public static void main(String [] args)
    {
        HBase       v_HBase  = new HBase("192.168.1.123");
        List<HData> v_HDatas = new ArrayList<HData>();
        
        log("1.1. 测试全表查询"   ,v_HBase.getRows("aa"));
//        log("1.2. 测试全表记录数" ,v_HBase.getCount("item"));
        
        
//        log("2.1. 测试全表查询(只显示某一列族的信息)"   ,v_HBase.getRows("item" ,new HData().setFamilyName("itemCF")));
//        log("2.2. 测试全表记录数(只显示某一列族的信息)" ,v_HBase.getCount("item" ,new HData().setFamilyName("itemCF")));
//        log("2.3. 测试全表查询(只显示某一列族的信息)"   ,v_HBase.getRows("item" ,new HData().setFamilyName("skuCF")));
//        log("2.4. 测试全表记录数(只显示某一列族的信息)" ,v_HBase.getCount("item" ,new HData().setFamilyName("skuCF")));
        
        
//        log("3.1. 测试一个条件的查询"   ,v_HBase.getRows("item"  ,new HData("" ,"itemType" ,"product0")));
//        log("3.2. 测试一个条件的记录数" ,v_HBase.getCount("item" ,new HData("" ,"itemType" ,"product0")));
        
        
//        v_HDatas.add(new HData("" ,"itemType" ,"product0"));
//        v_HDatas.add(new HData("" ,"examStatus" ,"0"));
//        log("4.1. 测试多个条件的查询"   ,v_HBase.getRows("item"  ,v_HDatas));
//        log("4.2. 测试多个条件的记录数" ,v_HBase.getCount("item" ,v_HDatas));
        
        
//        v_HDatas.add(new HData("" ,"itemType" ,"product0"));
//        v_HDatas.add(new HData("" ,"examStatus" ,"0").setOperator_OR());
//        log("5.1. 测试多个或关系条件的查询"   ,v_HBase.getRows("item"  ,v_HDatas));
//        log("5.2. 测试多个或关系条件的记录数" ,v_HBase.getCount("item" ,v_HDatas));
        
        
//        v_HDatas.add(new HData("" ,"action").setFamilyName("CF"));
//        log("6.1. 测试显示字段过滤的查询"   ,v_HBase.getRows("selectedItem"  ,v_HDatas));
//        log("6.2. 测试显示字段过滤的记录数" ,v_HBase.getCount("selectedItem" ,v_HDatas));
//        
//        v_HDatas.add(new HData("" ,"action").setFamilyName("CF"));
//        v_HDatas.add(new HData("" ,"action" ,"org.lazicats.icloud.platform.view.main.mock.ItemListService::getProductByRolePageList").setFamilyName("CF"));
//        log("6.3. 测试加查询条件的显示字段过滤的查询"   ,v_HBase.getRows("selectedItem"  ,v_HDatas));
//        log("6.4. 测试加查询条件的显示字段过滤的记录数" ,v_HBase.getCount("selectedItem" ,v_HDatas));
        
        
//        v_HDatas.add(new HData("i001"));
//        v_HDatas.add(new HData("i002"));
//        log("7.1. 测试RowKey的查询"   ,v_HBase.getRows("selectedItem"  ,v_HDatas));
//        log("7.2. 测试RowKey的记录数" ,v_HBase.getCount("selectedItem" ,v_HDatas));
        
        
//        logRow("8.1. 测试单一RowKey的查询" ,v_HBase.getRow("selectedItem"   ,"i001"));
//        log("8.2. 测试单一RowKey的记录数"  ,v_HBase.getCount("selectedItem" ,new HData("i001")));
        
//        log("9.1" ,v_HBase.getRows("item" ,new HData("" ,"isExit" ,"0").setFamilyName("itemCF") ,new HData("" ,"groupId" ,"group_20140513095242211").setFamilyName("itemCF")));
//        
        log("10.1. 测试获取单一记录中具体字段的值" ,v_HBase.getValue("item" ,new HData("item_2014051608550018" ,"itemBrandId").setFamilyName("itemCF")).toString());
    }
    
    
    
    public static void log(String i_TestDesc ,Map<String ,Map<String ,Object>> i_Datas)
    {
        System.out.println("\n\n-- " + i_TestDesc);
        
        int v_RowNo = 0;
        for (String v_Key : i_Datas.keySet())
        {
            Map<String ,Object> v_Row = i_Datas.get(v_Key);
            v_RowNo++;
            
            int v_ColNo = 0;
            for (String v_ColumnName : v_Row.keySet())
            {
                System.out.println("-- " + v_RowNo + "-" + (++v_ColNo) + ":" + v_Key + "\t" + v_ColumnName + " = " + v_Row.get(v_ColumnName));
            }
            
            System.out.println();
        }
    }
    
    
    
    public static void logRow(String i_TestDesc ,Map<String ,Object> i_Datas)
    {
        System.out.println("\n\n-- " + i_TestDesc);
        
        int v_ColNo = 0;
        for (String v_Key : i_Datas.keySet())
        {
            System.out.println("-- " + (++v_ColNo) + ":" + v_Key + " = " + i_Datas.get(v_Key));
        }
    }
    
    
    
    public static void log(String i_TestDesc ,long i_DataCount)
    {
        System.out.println("\n\n-- " + i_TestDesc + "\tRow Count = " + i_DataCount);
    }
    
    
    
    public static void log(String i_TestDesc ,String i_Data)
    {
        System.out.println("\n\n-- " + i_TestDesc + "\tValue = " + i_Data);
    }
    
}
