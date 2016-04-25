// Authors Karishma Raj, Nephi Calvin, Nikhil Sudireddy, Raghuveer Ramesh
package org.mapreduce.myhadoop;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.transfer.TransferManager;

/*
 * Custom Classes
 */
class Text {

	String val;
	public Text(String lowerCase) {

		this.val = lowerCase;
	}

	@Override
	public String toString() {
		return val;
	}


}

class LongWritable {
	String val;
	public LongWritable(String lowerCase) {

		this.val = lowerCase;
	}

	@Override
	public String toString() {
		return val;
	}

}

class Configuration {

}

class Reducer<T1, T2, T3, T4> {

}

class Mapper<T1, T2, T3, T4> {

}

class Job {

	public static Job getInstance(Configuration conf, String string) {
		return null;
	}

	public <M> void setMapperClass(Class<M> class1) {

	}

	public <R> void setReducerClass(Class<R> class1) {

	}

	public void setOutputKeyClass(Class<Text> class1) {

	}

	public void setOutputValueClass(Class<Text> class1) {

	}

}


class Context {

	static final Object lock = new Object();
	String reducerData;
	HashMap<String, ArrayList<String>> mapperData ;

	int mrNumber;
	int contextType;
	static final int MAPPER_TYPE = 1;
	static final int REDUCER_TYPE = 2;
	Text key;

	public Context(int mrNumber,int contextType,Text key) {

		this.reducerData = "";
		this.mrNumber = mrNumber;
		this.contextType = contextType;
		this.key = key;
		this.mapperData = new HashMap<String, ArrayList<String>>();
	}

	/*
	 * This is called for every context.write() in mapper, reducer
	 */
	public void write(Text mrkey, Text val) {


		if(contextType == REDUCER_TYPE){
			// Reducer
			reducerData = key.toString() + ", " + val.toString();
		}
		else{
			// Mapper
			String key1 = mrkey.toString();
			String val1 = val.toString();
			// Create HashMap based on Key
			if(mapperData.containsKey(key1))
			{
				ArrayList<String> value = mapperData.get(key1);
				value.add(val1);
				mapperData.put(key1, value);
			}
			else
			{
				ArrayList<String> value = new ArrayList<String>();
				value.add(val1);
				mapperData.put(key1, value);
			}
		}
	}

	/*
	 * This is called at the end of respective mapper, reducer
	 * Used to create a final Key File for both mapper, reducer
	 */
	public void writeToDisk(int mrNumber) throws IOException {


		if(contextType == REDUCER_TYPE){
			// Reducer
			TransferManager rtx = new TransferManager(
					new ProfileCredentialsProvider());

			File partFile = null;
			try {
				// New part file for each reducer Key
				partFile = new File("reducer-temp/part-r-"+key);

				if (!partFile.exists()) {
					partFile.createNewFile();
				}

				FileWriter fw = new FileWriter(partFile.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw);

				bw.write(reducerData);

				bw.close();
				fw.close();

			} catch (IOException e) {
				e.printStackTrace();
			}

			// Upload the part file to bucket
			rtx.upload(Slave.outBucket, partFile.getName()+".txt", partFile);

		}
		else
		{
			// Mapper
			TransferManager mtx = new TransferManager(
					new ProfileCredentialsProvider());
			// Create separate file for each key
			ArrayList<String> filesCurrent = new ArrayList<String>();
			File mapperPartFile = null;
			FileWriter fw;
			BufferedWriter bw = null;
			for(String key: mapperData.keySet())
			{
				mapperPartFile = new File("mapper-temp/M"+mrNumber+"_"+key+"_.txt");
				filesCurrent.add(mapperPartFile.getName());

				if (!mapperPartFile.exists()) {
					mapperPartFile.createNewFile();
				}

				fw = new FileWriter(mapperPartFile.getAbsoluteFile());
				bw = new BufferedWriter(fw);
				ArrayList<String> eachVal = mapperData.get(key);
				for(String v : eachVal)
				{
					bw.write(v+"\n");
				}
				bw.close();
				fw.close();
			}

			// Push all the mapper files onto intermediate bucket

			File dir = new File("mapper-temp/");
			File [] files = dir.listFiles();
			for (File mapperFile : files) {
				if(filesCurrent.contains(mapperFile.getName()))
					mtx.upload(Slave.interBucket, mapperFile.getName(), mapperFile);
			}
			synchronized(lock)
			{
				Slave.allMapperFileCount += mapperData.size();
			}
		}


	}


	/*
	 * This is used for pseudo mode
	 * Writing the final files onto local directory
	 */
	public void writeToLocalDisk(int mrNumber) throws IOException, InterruptedException{

		if(contextType == MAPPER_TYPE){

			// Create separate file for each key
			File mapperPartFile = null;
			FileWriter fw;
			BufferedWriter bw = null; 
			for(String key: mapperData.keySet())
			{
				mapperPartFile = new File(Pseudo.intermediateFolderPath+"/M"+mrNumber+"_"+key+"_.txt");

				if (!mapperPartFile.exists()) {
					mapperPartFile.createNewFile();
				}

				fw = new FileWriter(mapperPartFile.getAbsoluteFile());
				bw = new BufferedWriter(fw);
				ArrayList<String> eachVal = mapperData.get(key);
				for(String v : eachVal)
				{
					bw.write(v+"\n");
				}
				bw.close();
				fw.close();
			}	

			Pseudo.mapperInc();

			System.out.println("Finished mapper:"+mrNumber);
		}
		else{

			File partFile = null;
			try {

				partFile = new File(Pseudo.outputFolderPath+"/part-r-"+key);

				if (!partFile.exists()) {
					partFile.createNewFile();
				}

				FileWriter fw = new FileWriter(partFile.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw);

				bw.write(reducerData+"\n");

				bw.close();
				fw.close();

			} catch (IOException e) {
				e.printStackTrace();
			}

			Pseudo.reducerInc();
		}

	}
}
