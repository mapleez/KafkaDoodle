package org.dt.ez.kafka.tools.complex;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.dt.ez.kafka.doodle.partitions.SequenceData;

public class DataStrategy {
	
	private static String [] fileDatas;
//	private static SequenceData seqDatas;
	
	public static String [] parseFromArgument (String args) {
		if (args.startsWith ("file")) {
			if (fileDatas != null) 
				return fileDatas;
			String file = args.substring (args.indexOf (":"));
			return (fileDatas = initDataFromFile (file));
		}/* else if (args.startsWith ("seq")) {
			if (seqDatas != null)
				return seqDatas;
			long start = Long.parseLong (args.substring (args.indexOf (":")));
			SequenceData data = new SequenceData (start);
			
		}*/
		return null;
	}
	
	private static String [] initDataFromFile (String fileName) {
		final String encoding = "UTF-8";
		String [] datas = new String [0];
		List <String> container = new ArrayList <String> ();
		try {
	        File file = new File (fileName);
	        if (file.isFile () && file.exists ()) { //判断文件是否存在
	            InputStreamReader read = new InputStreamReader (
	            	new FileInputStream (file), encoding);// 考虑到编码格式
	            BufferedReader bufferedReader = new BufferedReader (read);
	            String lineTxt = null;
	            int i = 0; 
	            
	            while ((lineTxt = bufferedReader.readLine ()) != null && i <= 10000){
	            	
	            	String line = lineTxt.trim ();
	        		container.add (line);
	        		++ i;
	            }
	            read.close();
	            return container.toArray (datas);
	        }else{
	        	System.out.println ("Cannot found file " + fileName);
	        	System.exit (1);
	        }
	    } catch (Exception e) {
	        System.out.println ("Read File error");
	        e.printStackTrace ();
	        System.exit (1);
	    }
		return null;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
