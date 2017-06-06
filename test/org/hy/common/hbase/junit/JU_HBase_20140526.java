package org.hy.common.hbase.junit;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.hy.common.Execute;
import org.hy.common.JavaHelp;
import org.hy.common.hbase.HBase;






public class JU_HBase_20140526
{
    
    public static void main(String[] args)
    {
        JU_HBase_20140526 v_JU_HBase = new JU_HBase_20140526();
        HBase    v_HBase    = new HBase("192.168.105.107");
        
        
        for (int i=0; i<1; i++)
        {
            new Execute(v_JU_HBase ,"run" ,new Object[]{v_HBase ,Integer.valueOf(i)}).start();
        }
    }
    
    
    
    public void run(HBase i_HBase ,Integer i_No)
    {
        Map<String ,Object> v_FiliterMap = new Hashtable<String ,Object>();
        
        v_FiliterMap.put("groupPid" ,"group_00000000100");
        
        for (int i=0; i<1; i++)
        {
            this.goin(i_HBase ,i_No ,toList(i_HBase.getRows("group" ,v_FiliterMap)) ,"group_00000000100");
        }
    }
    
    
    
    public void goin(HBase i_HBase ,Integer i_No ,List<Map<String ,Object>> i_Groups ,String i_Pid)
    {
        if ( JavaHelp.isNull(i_Groups) )
        {
            return;
        }
        
        for (int i=0; i<i_Groups.size(); i++)
        {
            String v_GroupID  = i_Groups.get(i).get("groupId").toString();
            
            if ( !v_GroupID.equals(i_Pid) )
            {
                Map<String ,Object> v_FiliterMap = new Hashtable<String ,Object>();
                
                v_FiliterMap.put("groupPid" ,v_GroupID);
                
                
                List<Map<String ,Object>> v_Childs = toList(i_HBase.getRows("group" ,v_FiliterMap));
                
                System.out.println("-- " + i_No + " : " + v_Childs);
                
                this.goin(i_HBase ,i_No ,v_Childs ,v_GroupID);
            }
        }
    }
    
    
    
    public static List<Map<String ,Object>> toList(Map<String ,Map<String ,Object>> i_Map)
    {
        List<Map<String ,Object>> v_Ret = new ArrayList<Map<String ,Object>>();
        
        for (Map<String ,Object> v_Row : i_Map.values())
        {
            v_Ret.add(v_Row);
        }
        
        return v_Ret;
    }
    
}
