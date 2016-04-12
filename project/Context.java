import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.transfer.TransferManager;

class Context {

	Map<Text, List<Text>> dict;
	
	List<Text> result;
	
	int reducerNumber;
	int contextType;
	static final int MAPPER_TYPE = 1;
	static final int REDUCER_TYPE = 2; 
	Text key;
	String outputBucketName;

	public Context(int rNumber,int contextType,Text key,String outputBucketName) {

		this.dict = new TreeMap<Text, List<Text>>();
		this.result = new ArrayList<Text>();
		this.reducerNumber = rNumber;
		this.contextType = contextType;
		this.key = key;
		this.outputBucketName = outputBucketName;
	}

	public void write(Text key, Text val) {

		
		if(contextType == REDUCER_TYPE){
			
			result.add(val);
			
		}
		else{
			
		}		 

	}

	public void writeToDisk() {
		
		
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
			
			TransferManager tx = new TransferManager(
					new ProfileCredentialsProvider());
			
				
			tx.upload(outputBucketName, partFile.getName()+".txt", partFile);
			
			System.out.println("Uploaded part file to bucket");

		}
		
	}
	
	

}