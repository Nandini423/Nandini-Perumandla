package com.wicore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import java.util.Properties;
import java.util.TimeZone;

import javax.sql.DataSource;

import dao.ConnectionDAO;

public class CSVFile {
    static CharsetEncoder asciiEncoder = 
    Charset.forName("US-ASCII").newEncoder(); // or "ISO-8859-1" for ISO Latin 1
    public static boolean isPureAscii(String v) 
    {
    	return asciiEncoder.canEncode(v);
    }  
    public static void main(String[] args) throws FileNotFoundException, SQLException 
    {
    	Connection con = null;
    	Statement st = null, stmt = null, stm=null;
	  	  ResultSet rs = null;
	  	  PreparedStatement pst = null;
	  	  String[] str = new String[102];
	  	  String str_query = "";
		Calendar cl = null, c2=null;
		DataSource ds = null;
		Date db_date; 
		System.out.println(".....................");
		 int commit_count = 0;
	    try 
		{
			//con.setAutoCommit(false);
	    	commit_count = 0;
			con=  ConnectionDAO.getCon();
			st = con.createStatement();
			//int commit_count = 0;
			con.setAutoCommit(false);
			int batchSize = 20;
	    	//File path = new File("E:\\CSV_FILES");
			// your date
			// Choose time zone in which you want to interpret your Date
			db_date = new Date();
			cl = Calendar.getInstance();
			cl.set(Calendar.HOUR_OF_DAY, 0);
			cl.set(Calendar.MINUTE, 0);
			cl.set(Calendar.SECOND, 0);
			cl.set(Calendar.MILLISECOND, 0);
			String query="SELECT STR_TO_DATE(s_value, '%d-%M-%Y')  FROM settings WHERE s_field='SUMMARYACTION'";
			   rs = st.executeQuery(query);
			   if (rs.next()){ 
				  db_date = rs.getTimestamp(1);
			   }
			   System.out.println("the date=" + db_date);
			   System.out.println(" date=" + cl.getTime());
			   while (db_date.before(cl.getTime()) ) 
			   {
			   System.out.println("db_date:::::"+db_date);
			   cl.setTime(db_date);
			   String location ="E:\\CSV_FILES";
			    //File [] files = path.listFiles();
			    //System.out.println("files::::"+files);
			    File folder = new File("E:/CSV_FILES");
			    File[] listOfFiles = folder.listFiles();
				int year = cl.get(Calendar.YEAR);
				int month = cl.get(Calendar.MONTH);
				int day = cl.get(Calendar.DAY_OF_MONTH);
			   System.out.println("the date=" + db_date);
			   String year1=Integer.toString(year);
			   String month1="";
			   if(month<10) {
			   month1="0"+Integer.toString(month);
			   }else {
				   month1=Integer.toString(month);
			   }
			   String day1="";
			   if(day<10) {
				   day1="0"+Integer.toString(day);
			   }else {
				   day1=Integer.toString(day); 
			   }
			   System.out.println("day::::"+day);
			   System.out.println("month::::"+month);
			   System.out.println("year::::"+year);
			   System.out.println(" date=" + cl.getTime());
			   String fileNamee="";
			   String command="sms_history_"+year1+month1+day1;
			   System.out.println("command:::"+command);
			   System.out.println("..................");
			    for (File file : listOfFiles) {
				       System.out.println(file.getName());
			           String fileName=file.getName();
			        	fileNamee=fileName;
			        	System.out.println("fileNamee:::"+fileNamee);
				       FileReader csvFilePath = new FileReader(location+"/"+fileNamee);
					    String sql = "INSERT INTO sms_history (id, msisdn, mid, op_id, circle_id, circle_name, order_id, wipay_trans_id, short_code, current_action, current_action_desc, status, status_msg, created_ts, payload, errorcode, node, url, price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
						   System.out.println("sql::::"+sql);
				           PreparedStatement statement = con.prepareStatement(sql);
					       BufferedReader lineReader = new BufferedReader(csvFilePath);
				           String lineText = null;
				           int count = 0;
				           lineReader.readLine();
				           while ((lineText = lineReader.readLine()) != null) {
				               String[] data = lineText.split(",");
				               String id=data[0];
				               System.out.println("id::::"+id);
				               String msisdn = data[1];
				               String msisdn1=msisdn.replace("\"", "");
				               //System.out.println("msisdn::::"+msisdn1);
				               String mid = data[2];
				               String op_id = data[3];
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
				               //System.out.println("created_ts1::"+created_ts1);
				               Timestamp time=Timestamp.valueOf(created_ts1);
				               //System.out.println("time:::"+time);
				               String node = data[data.length - 3];
				               //System.out.println("node:::"+node);
				              if(node.equals("\\N")) {
				            	   node="0";
				               }else {
				            	   node=data[16];
				               }
				               int node1=Integer.parseInt(node);
				               statement.setString(1, id);
				               statement.setString(2, msisdn1);
				               statement.setString(3,  mid);
				               statement.setString(4, op_id);
				               statement.setString(5, circle);
				               statement.setString(6, circle_name1);
				               statement.setString(7, order_id1);
				               statement.setString(8, wipay_trans_id1);
				               statement.setString(9, short_code1);
				               statement.setString(10, current_action1);
				               statement.setString(11, current_action_desc1);
				               statement.setString(12, status1);
				               statement.setString(13, status_msg);
				               statement.setTimestamp(14, time);
				               statement.setString(15, null);
				               statement.setString(16, null);
				               statement.setInt(17,node1);
				               statement.setString(18, null);
				               statement.setString(19, null);
				               str[commit_count] = sql ;
				  		          statement.addBatch();
				               if (count % batchSize == 0) {
				                   statement.executeBatch();
				                   statement.clearBatch();
							       con.commit();
				               }
					           }
				           System.out.println("*************************");
				           lineReader.close();
				           statement.executeBatch();
				           try {
				  			   commit_count = 0;
				  			   stmt = con.createStatement();
				  			   st = con.createStatement();
				  			   String qry="select if(op_id is null or op_id = '' or op_id = '0' , '-1', op_id) as sms_op_id,if(mid is null or mid = '' or mid = '0' ,  '-1', mid) as sms_mid,circle_id ,circle_name,current_action, node, short_code, status, Hour (created_ts) as hour, count(*)  as action_count, created_ts from sms_history group by DATE(created_ts), hour(created_ts),short_code , op_id,circle_name,current_action";
				  			   System.out.println("qry::::"+qry);
				  			   rs = st.executeQuery(qry);
				  			   while (rs.next()){
				  				   System.out.println("----------------------->");
				  				   try{
					  					 String mid2=rs.getString("sms_mid") ;
					  				 	 String opid=rs.getString("sms_op_id") ;
					  					 String circleid=rs.getString("circle_id");
					  					 String node2=rs.getString("node");
					  					 String shortcode=rs.getString("short_code");
					  					 String count2=rs.getString("action_count");
					  					 String hour=rs.getString("hour");
					  					 /*if(!org.apache.commons.lang.StringUtils.isNumeric(mid))
					  					 {
					  				    	mid="-1";
					  				    	//System.out.println("mid::"+mid);
					  					 }
					  					 if(!org.apache.commons.lang.StringUtils.isNumeric(opid))
					  					 {
					  					   opid="-1";
					  					   //System.out.println("opid::"+opid);
					  					 }*/
					  					 if(node2==null) 
					  					 {
					  						 node2="0";
					  					 }
					  					System.out.println(mid2+""+opid+""+circleid+""+node2+""+shortcode+""+count2);
			  					 str_query = "insert into sms_history_aggregation_hourly (mid, op_id, circle_id, circle_name , current_action, short_code, node, status, hour, count, created_ts) values('" + mid2 + "','" + opid +"','" +circleid + "','" + rs.getString("circle_name") + "','" + rs.getString("current_action") + "','" + shortcode + "','" + node2 +"','" + rs.getString("status") + "','" + hour+"','" +count2+ "','" +rs.getDate("created_ts") +"')";
			  					 System.out.println("str_query::"+str_query);					  
			  					 str[commit_count] = str_query;
			  					 stmt.addBatch(str_query);
			  					 if (commit_count++ > 100)
			  					 {
			  				       stmt.executeBatch();
			  				       stmt.clearBatch();
			  				       con.commit();
			  				       commit_count = 0;
			  					 }
			  			   	}catch(Exception e){
			  				   System.out.println("Exception while inserting SMSHistoryAggregation- " + e);
			  				   e.printStackTrace();	 
			  			    }
			  			 }
			  		     if (commit_count > 0){
			  			     stmt.executeBatch();
			  			     stmt.clearBatch();
			  			     con.commit();
			  			     commit_count = 0;
			  		     }
			         }catch (java.sql.BatchUpdateException e){
			      	   try {
			  	    	con.commit();
			      	   } 
			      	   catch (Exception ce){
			  			System.out.println("B.U.E : " + ce);
			      	   }
			         } catch (Exception e){
			      	   try{
			      		   con.rollback();
			      	   } catch (Exception se) {
			      		   System.out.println("Exception in daily_summary WeekDayScheduler :: " + se);
			      		   se.printStackTrace();
			      	   }
			      	   System.out.println("Exception in daily_summary WeekDayScheduler- " + e);
			      	   e.printStackTrace();
			         }
			  	  if (rs != null) {
			  		rs.close();
			  		rs = null;
			  	  }if (stmt != null){
			  		 stmt.close();
			  		 stmt = null;
			  	  }if (st != null){
			  	     st.close();
			  	     st = null;
			  	  }
			  	stm=con.createStatement();
		        stm.executeUpdate("TRUNCATE TABLE sms_history");
		        System.out.println("Truncated");  
			  	  if (stm != null){
			  	     stm.close();
			  	     stm = null;
			        }
			  	  	//con.commit();
			  }
			    System.out.println("Data inserted successufully...");
		          c2 = Calendar.getInstance();
		          c2.setTime(db_date);
		    	  c2.add(Calendar.DATE, 1);
		    	  c2.set(Calendar.HOUR_OF_DAY, 0);
		    	  c2.set(Calendar.MINUTE, 0);
		    	  c2.set(Calendar.SECOND, 0);
		    	  db_date = c2.getTime();
		    	  st = con.createStatement();
		    	  String str1="update settings set s_value='" + new SimpleDateFormat("dd-MMM-yyyy").format(db_date) + "' WHERE  s_field='SUMMARYACTION'";
		    	 System.out.println("Date updated:::"+str1);
		    	  st.executeUpdate(str1);
		    	  System.out.println("the date=" + db_date);
		    	  System.out.println(" date=" + c2.getTime());
		    	  if (st != null){
		    	     st.close();
		    	     st = null;
		          }
		    	  	con.commit();
			   } //end of while
	System.out.println("SMSHistory HourlyWise Aggregation process completed for the date=" + db_date);
		    	} catch (Exception e) {
		    	   try {
		    		   con.rollback();
		    	   } catch (Exception se) {
		    		   System.out.println("Exception  :: " + se);
		    		   se.printStackTrace();
		           }
		    	   System.out.println("Exception- " + e);
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
		    	    if (con != null){
		    	     con.close();
		    	     con = null;
		    	    }
		    	    ds = null;
		    	    cl = null;
		    	    c2 = null;
		    	    db_date = null;
		       } catch (Exception e){
		    	   System.out.println("Exception in SMSHistoryHourlyWiseAggregationMain -- " + e);
		       }
	}	
}
}


