import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.transfer.TransferManager;

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

	/*public void setJarByClass(Class<Alice> class1) {

	}*/

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

	Map<Text, List<Text>> dict;

	List<Text> result;
	//HashMap<Text, ArrayList<Text>> mapperData = new HashMap<Text, ArrayList<Text>>();
	HashMap<String, ArrayList<String>> mapperData ;

	int mrNumber;
	int contextType;
	static final int MAPPER_TYPE = 1;
	static final int REDUCER_TYPE = 2;
	Text key;
	String outputBucketName;

	public Context(int mrNumber,int contextType,Text key,String outputBucketName) {

		this.dict = new TreeMap<Text, List<Text>>();
		this.result = new ArrayList<Text>();
		this.mrNumber = mrNumber;
		this.contextType = contextType;
		this.key = key;
		this.outputBucketName = outputBucketName;
		this.mapperData = new HashMap<String, ArrayList<String>>();
	}

	public void write(Text key, Text val) {

		
		if(contextType == REDUCER_TYPE){

			result.add(val);

		}
		else{
			String key1 = key.toString();
			String val1 = val.toString();
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

	public void writeToDisk(int mrNumber) throws IOException {


		if(contextType == REDUCER_TYPE){
			File partFile = null;
			try {

				partFile = new File("out/part-r-"+key);

				if (!partFile.exists()) {
					partFile.createNewFile();
				}

				FileWriter fw = new FileWriter(partFile.getAbsoluteFile());
				BufferedWriter bw = new BufferedWriter(fw);

				for(Text t : result){
					bw.write(key.toString()+","+t.toString());
				}

				bw.close();
				fw.close();

				System.out.println("Wrote partfile to disk");

			} catch (IOException e) {
				e.printStackTrace();

			}

			TransferManager rtx = new TransferManager(
					new ProfileCredentialsProvider());


			rtx.upload(outputBucketName, partFile.getName()+".txt", partFile);

			System.out.println("Uploaded part file to bucket");

		}
		else
		{
			TransferManager mtx = new TransferManager(
					new ProfileCredentialsProvider());
			// Create separate file for each key
			//PrintWriter mapperPartFile = null;
			File mapperPartFile = null;
			System.out.println("Trying to write for:"+mrNumber);
			FileWriter fw;
			BufferedWriter bw = null; 
			for(String key: mapperData.keySet())
			{
				//partFile = new File("out/part-r-"+key);
				mapperPartFile = new File("temp/M"+mrNumber+"_"+key+"_.txt");

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
			
			System.out.println("Finished writing mapper files into local disk");
			/*
			 * 
			
			File dir = new File(".");
			File [] files = dir.listFiles(new FilenameFilter() {
			    @Override
			    public boolean accept(File dir, String name) {
			        return name.endsWith(".txt");
			    }
			});
			for (File mapperFile : files) {
				if(!mapperFile.getName().equals("publicDNS.txt"))
				{
					mtx.upload(outputBucketName, mapperFile.getName(), mapperFile);
				}
			} */
			System.out.println("Finished pushing mapper files into S3 intermediate bucket");
		}

	}


}
