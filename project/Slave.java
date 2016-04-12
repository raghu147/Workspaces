import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Slave {
	static HashMap<Integer, String> ser_port_hash = new HashMap<Integer, String>();

	public static void main(String[] args) {
		int port = Integer.parseInt(args[0]);
		String inputBucket;
		String outputBucket = "project-bucket-cs6240-out";
		String intermediateBucket = "project-bucket-cs6240-int";
		ServerSocket serverSock = null;
		Socket s = null;	
		try
		{
			serverSock = new ServerSocket(port);
			System.out.println("Server listening at port: "+port);
			while(true)
			{
				s = serverSock.accept();
				BufferedReader inFromClient = new BufferedReader(new InputStreamReader(s.getInputStream()));
				String line = inFromClient.readLine();

				if(line.startsWith("BUCKET_INFO")){
					String buckets = line.split(":")[1];
					inputBucket = buckets.split(",")[0];
					outputBucket = buckets.split(",")[1];
					System.out.println("Bucket info is ........" + line);
				}

				if(line.startsWith("SERVER_PORT_DNS_LIST")){
					String s_p_line = line.split(":")[1];
					String server_port_list[] = s_p_line.split(",");
					for(String sp : server_port_list)
					{
						// server_num, port#dns
						ser_port_hash.put(Integer.parseInt(sp.split("=")[0]), sp.split("=")[1]);
					}
					System.out.println("Server port info is ........" + line);
				}

				if(line.startsWith("MAPPER_INFO")){
					System.out.println("Mapper info in slave is ........" + line);
				}

				if(line.startsWith("KILL_YOURSELF")){
					System.out.println("Killing myself.....");
					break;
				}

				if(line.startsWith("DO_REDUCE")){

					ReducerThread obj = new ReducerThread(intermediateBucket,outputBucket,line.split(":")[1]);
					Thread t = new Thread(obj);
					t.start();
				}

			}
			s.close();
			serverSock.close();

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}

}

class ReducerThread extends Thread{

	List<String> keyList;
	String outputPath ;
	String inputPath;
	String message;


	public ReducerThread(String inputPath,String outputPath,String message){
		this.outputPath = outputPath;
		this.inputPath = inputPath;
		this.message = message;
		keyList = new ArrayList<String>();

	}

	public void run(){



		if(message.contains("#")){

			String splits[] = message.split("#");

			for(String x : splits){
				keyList.add(x);
			}

		}
		else{
			keyList.add(message);
		}

		for(String key : keyList){

			// For each key create a Reducer
			Context ctx = new Context(0, Context.REDUCER_TYPE, new Text(key),outputPath);
			ReducerTask task = new ReducerTask(inputPath,outputPath,key,ctx);
			Thread t = new Thread(task);
			t.start();
		}


	}

}


// One reducer
class ReducerTask extends Thread{

	static AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
	String fileKey;
	String inputPath;
	String outputPath;
	Context context;


	public ReducerTask(String inputPath,String outputPath,String key,Context ctx){
		this.outputPath = outputPath;
		this.inputPath = inputPath;
		this.fileKey = key;
		this.context = ctx;

	}
	public void run(){

		ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(inputPath);
		ObjectListing objectListing;


		//Create object
		Class<?> c = null;
		Method method = null;
		try {
			
			
			c = Class.forName("Alice$R");
			method = c.getMethod("reduce",new Class[] { Text.class,List.class,Context.class});
			
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}



		List<String> fileList = new ArrayList<String>();

		do {
			objectListing = s3Client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : 
				objectListing.getObjectSummaries()) {

				String fileName=objectSummary.getKey();
				System.out.println(fileName);
				if(fileName.equals("project-bucket-cs6240-int/"))
					continue;

				
				if(fileName.split("_")[0].equals(fileKey)){			
					fileList.add(fileName);					
				 
				}

			}

			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());
		
		
		
		List<Text> toReducer = new ArrayList<Text>();
		
		for(String f : fileList){
			S3Object s3object = s3Client.getObject(new GetObjectRequest(
					"project-bucket-cs6240-int", f));
			
			System.out.println("F="+f);
			BufferedReader reader = new BufferedReader(new InputStreamReader(s3object.getObjectContent()));
			String line;
			try {
				while((line = reader.readLine()) != null) {
					toReducer.add(new Text(line));
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		}
		
		try {
			method.invoke(c, new Text(fileKey),toReducer,context);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}

		context.writeToDisk();

	}


}




