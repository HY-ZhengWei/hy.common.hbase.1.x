package org.hy.common.hbase.junit;

import java.io.IOException;

import org.hy.common.file.FileHelp;
import org.hy.common.hbase.HBase;





/**
 * 初始化 CW-Portal项目的数据库数据
 * 
 * @author ZhengWei(HY)
 * @create 2014-05-16
 */
public class Init_CW_Portal 
{

	public static void main(String[] args) throws NullPointerException, IOException 
	{
	    HBase    v_HBase    = new HBase("192.168.105.107");
		
		FileHelp v_FileHelp = new FileHelp();
		
		String v_Text = v_FileHelp.getContent(Init_CW_Portal.class.getResource("HBase_Temp.sql") ,"UTF-8");
		
		String [] v_Commands = v_Text.split("put");
		
		for (int i=0; i<v_Commands.length; i++)
		{
			if ( v_Commands[i] == null || "".equals(v_Commands[i].trim()) )
			{
				
			}
			else
			{
				String [] v_Params = v_Commands[i].split(",");
				
				if ( v_Params.length == 4 )
				{
					String v_TableName  = v_Params[0].trim().replaceAll("'", "");
					String v_RowKey     = v_Params[1].trim().replaceAll("'", "");
					String v_FamilyName = v_Params[2].trim().replaceAll("'", "").split(":")[0];
					String v_ColumnName = v_Params[2].trim().replaceAll("'", "").split(":")[1];
					String v_Value      = v_Params[3].trim().replaceAll("'", "").replaceAll(";", "");
					
					v_HBase.put(v_TableName
    			               ,v_RowKey
    			               ,v_FamilyName
    			               ,v_ColumnName
    			               ,v_Value);
					
					System.out.println("-- Info: " + v_TableName + "." + v_RowKey + "." + v_FamilyName + ":" + v_ColumnName + " = " + v_Value);
				}
				else
				{
					System.out.println("\n-- Error: " + v_Commands[i] + "\n");
				}
			}
			
		}
	}

}
