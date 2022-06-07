package com.wicore;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.sql.DataSource;

import dao.ConnectionDAO;

public class ReadFilesInCSV {
	 static CharsetEncoder asciiEncoder = 
			    Charset.forName("US-ASCII").newEncoder(); // or "ISO-8859-1" for ISO Latin 1
    public static boolean isPureAscii(String v) 
    {
    	return asciiEncoder.canEncode(v);
    }  
	    public static void main(String[] args) throws SQLException, IOException 
	    {
	    	  Connection con = null;
	    	  Statement st = null, stmt = null, stm=null;
		  	  ResultSet rs = null;
		  	  PreparedStatement pst = null;
		  	  String[] str = new String[102];
		  	  String str_query = "";
	  	BufferedReader[] b1=new BufferedReader[2];
		b1[0]=new BufferedReader(new FileReader("E:/CSV_FILES/sms_history_54767_55321_bill_requests.csv"));
		b1[1]=new BufferedReader(new FileReader("E:/CSV_FILES/sms_history_20220410_20220418001003.csv"));
	  	  try{
	  		  con=  ConnectionDAO.getCon();
	  		  int commit_count = 0;
	  		   con.setAutoCommit(false);
	  		   boolean flag=false;
	  		   while(!flag) {
					for(BufferedReader b2:b1)
					{
							//String line=b2.readLine();
						   String sql = "INSERT INTO sms_history (msisdn, mid, op_id, circle_id, circle_name, order_id, wipay_trans_id, short_code, current_action, current_action_desc, status, status_msg, created_ts, payload, errorcode, node, url, price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
						   System.out.println("sql::::"+sql);
				           PreparedStatement statement = con.prepareStatement(sql);
					     	int count = 0;
					     	String lineText=null;
				           while ((lineText = b2.readLine()) != null) {
				               String[] data = lineText.split(",");
				               //String id=data[0];
				               //System.out.println("id::::"+id);
				               String msisdn = data[1];
				               String msisdn1=msisdn.replace("\"", "");
				               System.out.println("msisdn::::"+msisdn1);
				               String mid = data[2];
				               System.out.println("mid::::"+mid);
				               if(mid.equalsIgnoreCase("mid")) {
				            	   mid="0";
				               }
				               System.out.println("mid:::111:"+mid);
				               //int mid1=Integer.parseInt(mid);
				              // System.out.println("mid1::::"+mid1);
				               String op_id = data[3];
				               //int op_id1=Integer.parseInt(op_id);
				               String circle_id = data[4];
				               String circle=circle_id.replace("\"", "");
				               String circle_name = data[5];
				               String circle_name1=circle_name.replace("\"", "");
				               String order_id = data[6];
				               String order_id1=order_id.replace("\"", "");
				               String wipay_trans_id = data[7];
				               String wipay_trans_id1=wipay_trans_id.replace("\"", "");
				               String short_code = data[8];
				               String short_code1=short_code.replace("\"", "");
				               String current_action = data[9];
				               String current_action1=current_action.replace("\"", "");
				               String current_action_desc = data[10];
				               String current_action_desc1=current_action_desc.replace("\"", "");
				               String status = data[11];
				               String status1=status.replace("\"", "");
				               String status_msg = data[12];
				               String created_ts = data[13];
				               String created_ts1=created_ts.replace("\"", "");
				               System.out.println("created_ts1::"+created_ts1);
				               Timestamp time=Timestamp.valueOf(created_ts1);
				               System.out.println("time:::"+time);
				               //String payload = data[14];
				               //String payload1=payload.replace("\"", "");
				               //String errorcode = data[15];
				               //String errorcode1=errorcode.replace("\"", "");
				               String node = data[data.length - 3];
				               System.out.println("node:::"+node);
				              if(node.equals("\\N")) {
				            	   node="0";
				               }else {
				            	   node=data[16];
				               }
				               	//int node1=Integer.parseInt(node);
				               Sms_history sms=new Sms_history();
				               //statement.setString(1, id);
				               sms.setMsisdn(msisdn1);
				               sms.setMid("0");
				               sms.setOp_id(op_id);
				               sms.setCircle_id(circle);
				               sms.setCircle_name(circle_name1);
				               sms.setOrder_id(order_id1);
				               sms.setWipay_trans_id(wipay_trans_id1);
				               sms.setShort_code(short_code1);
				               sms.setCurrent_action(current_action1);
				               sms.setCurrent_action_desc(current_action_desc1);
				               sms.setStatus(status1);
				               sms.setStatus_msg(status_msg);
				               sms.setCreated_ts(time);
				               sms.setErrorcode(null);
				               sms.setPayload(null);
				               sms.setNode(node);
				               sms.setUrl(null);
				               sms.setPrice(null);
				           }
	     } //end of while
		
	  }
	  	} catch (Exception e) {
	  	   try {
	  		   con.rollback();
	  	   } catch (Exception se) {
	  		   System.out.println("Exception in :: " + se);
	  		   se.printStackTrace();
	         }
	  	   System.out.println("Exception in - " + e);
	  	   e.printStackTrace();
	    } finally{
	     try {
	  	    if (rs != null){
	  	     rs.close();
	  	     rs = null;
	  	    }
	  	    if (st != null) {
	  	     st.close();
	  	     st = null;
	  	    }
	  	    if (stmt != null){
	  	     stmt.close();
	  	     stmt = null;
	  	    }
	  	    if (pst != null) {
	  	     pst.close();
	  	     pst = null;
	  	    }
	  	    if (con != null){
	  	     con.close();
	  	     con = null;
	  	    }
	     } catch (Exception e){
	  	   System.out.println("Exception in SMSHistoryHourlyWiseAggregationMain -- " + e);
	     }
	    }
	   }
	  }