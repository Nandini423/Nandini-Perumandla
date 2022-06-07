package com.wicore;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import dao.ConnectionDAO;

public class CSVFileToSmsHistoryTable {
	private static List<CSVFileToSmsHistoryTable> processInputFile() throws FileNotFoundException, SQLException
	{
		//String inputFilePath="E:/CSV_FILES";

	    List<CSVFileToSmsHistoryTable> inputList = new ArrayList<CSVFileToSmsHistoryTable>();
	    Connection con = null;
    	Statement st = null, stmt = null, stm=null;
     	String[] str = new String[102];
     	int commit_count = 0;
 	    try 
 		{
 			//con.setAutoCommit(false);
 	    	commit_count = 0;
		    con=  ConnectionDAO.getCon();
			st = con.createStatement();
			con.setAutoCommit(false);
			int batchSize = 100;
			String location ="E:\\FILES";
	    	//File folder = new File("E:/CSV_FILES");
			//File inputF = new File("E:/CSV_FILES");
			//System.out.println("inputF::::"+inputF);
			File folder = new File("E:\\FILES");
		    File[] listOfFiles = folder.listFiles();
		    System.out.println("listOfFiles::::"+listOfFiles);
		    String fileNamee="";
		    for (File file : listOfFiles) {
			       System.out.println(file.getName());
			       String fileName=file.getName();
		        	fileNamee=fileName;
		        	String command=location+"/"+fileNamee;
		        	System.out.println("command"+command);
		        	//String command="E:/CSV_FILES/sms_history_54767_55321_bill_requests.csv";
		        	//System.out.println("listOfFiles::::"+listOfFiles);
					InputStream inputFS = new FileInputStream(command);
					//BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
					// skip the header of the csv
					//inputList = br.lines().map(mapToItem).collect(Collectors.toList());
					//System.out.println("inputList::::"+inputList);
					//int size=inputList.size();
					//System.out.println("size:::"+size);
					String sql = "INSERT INTO sms_history (id, msisdn, mid, op_id, circle_id, circle_name, order_id, wipay_trans_id, short_code, current_action, current_action_desc, status, status_msg, created_ts, payload, errorcode, node, url, price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
					   System.out.println("sql::::"+sql);
			           PreparedStatement statement = con.prepareStatement(sql);
				       BufferedReader lineReader = new BufferedReader(new InputStreamReader(inputFS));
			           String lineText = null;
			           int count = 0;
			           lineReader.readLine();
			           while ((lineText = lineReader.readLine()) != null) {
			               String[] data = lineText.split(",");
			               String id=data[0];
			               System.out.println("id::::"+id);
			               String msisdn = data[1];
			               String msisdn1=msisdn.replace("\"", "");
			               System.out.println("msisdn::::"+msisdn1);
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
			               System.out.println("created_ts1::"+created_ts1);
			               Timestamp time=Timestamp.valueOf(created_ts1);
			               System.out.println("time:::"+time);
			               String node = data[data.length - 3];
			               System.out.println("node:::"+node);
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
			  		          statement.addBatch();
			               if (count % batchSize == 0) {
			                   statement.executeBatch();
			               }
			           }
			           System.out.println("*************************");
			           lineReader.close();
			           statement.executeBatch();
			           con.commit();
			           con.close();
			          System.out.println("Data inserted successufully...");
		    }
			//File[] listOfFiles = inputF.listFiles();
			
			//String file="E:/CSV_FILES";
			//FileInputStream inputFS = new FileInputStream(file);
			//System.out.println("inputFS::::"+inputFS);
 		
	    } catch (IOException e) {
	     System.out.println("e:::::"+e);
	    }
	    return inputList ;
	}
	private static Function<String, CSVFileToSmsHistoryTable> mapToItem = (line) -> {
		  String[] p = line.split(",");// a CSV has comma separated lines
		  CSVFileToSmsHistoryTable item = new CSVFileToSmsHistoryTable();
		  item.setItemNumber(p[0]);//<-- this is the first column in the csv file
		  if (p[3] != null && p[3].trim().length() > 0) {
		    item.setSomeProeprty(p[3]);
		  }
		  return item;
		};
	private void setItemNumber(String string) {
		// TODO Auto-generated method stub
		
	}
	private void setSomeProeprty(String string) {
		// TODO Auto-generated method stub
		
	}
	public static void main(String[] args) throws FileNotFoundException, SQLException {
		processInputFile(); // Method being called.
		 
   }
}
