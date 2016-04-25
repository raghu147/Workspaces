// Authors Karishma Raj, Nephi Calvin, Nikhil Sudireddy, Raghuveer Ramesh
package org.mapreduce.myhadoop;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.zip.GZIPInputStream;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Slave {
	public static ClientConfiguration clientConfig = new ClientConfiguration();
	static HashMap<Integer, String> ser_port_hash = new HashMap<Integer, String>();
	static int allMapperFileCount = 0;
	static String className = "";
	static int mapperCount = 0;
	static int reducerCount = 0;
	private static int reducerFinishedCount = 0;
	private static int mapperFinishedCount = 0;
	public static String inputBucket ="";
	public static String inBucket = "";
	public static String inPrefix = "";
	public static String outputBucket = "";
	public static String outBucket = "";
	public static String outPrefix = "";
	public static String intermediateBucket = "";
	public static String interBucket = "";
	public static String interPrefix = "";

	/*
	 * Synchronized methods to increment static variables
	 * These variables are used to determine the number of
	 * reducers, mappers finished in a particular machine
	 */
	public static synchronized int getReducerFinishedCount() {
		return reducerFinishedCount;
	}

	public static synchronized void incReducerFinishedCount() {
		Slave.reducerFinishedCount ++;
	}

	public static synchronized int getMapperFinishedCount() {
		return mapperFinishedCount;
	}

	public static synchronized void incMapperFinishedCount() {
		Slave.mapperFinishedCount++;
	}

	public static void main(String[] args) {
		clientConfig.setConnectionTimeout(50*10000);
		int port = Integer.parseInt(args[0]);
		int already_sent_mapper = 0;
		int already_sent_reducer = 0;
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
					/*
					 * Parse bucket names, to get bucket, its prefix
					 */
					String buckets = line.split(":")[1];
					inputBucket = buckets.split(",")[0];
					inBucket = inputBucket.split("#")[0];
					inPrefix = inputBucket.split("#")[1].equals("empty") ? "": inputBucket.split("#")[1];
					intermediateBucket = buckets.split(",")[1];
					interBucket = intermediateBucket.split("#")[0];
					interPrefix = intermediateBucket.split("#")[1].equals("empty") ? "": intermediateBucket.split("#")[1];
					outputBucket = buckets.split(",")[2];
					outBucket = outputBucket.split("#")[0];
					outPrefix = outputBucket.split("#")[1].equals("empty") ? "": outputBucket.split("#")[1];
					System.out.println("Bucket info is ........" + line);
				}

				if(line.startsWith("SERVER_PORT_DNS_LIST")){
					// Get List of port info
					String s_p_line = line.split(":")[1];
					String server_port_list[] = s_p_line.split(",");
					for(String sp : server_port_list)
					{
						// Format: server_num, port#dns
						ser_port_hash.put(Integer.parseInt(sp.split("=")[0]), sp.split("=")[1]);
					}
					System.out.println("Server port info is ........" + line);
				}

				if(line.startsWith("MAPPER_INFO")){
					// Start Mapper Thread
					mapperCount = line.split("#").length;
					className = line.split(":")[2];
					MapperThread mobj = new MapperThread(line.split(":")[1]);
					Thread mt = new Thread(mobj);
					mt.start();
				}

				if(line.startsWith("GET_MAPPER_STATUS"))
				{
					/*
					 *  Master is requesting for Mapper Status
					 *  Returns 1, if the mapper phase is done in this machine else 0
					 *  To Check if mapper phase is done, it checks,
					 *  number of mappers received and the number of mappers finished execution.
					 */
					Socket clientSocket = new Socket(ser_port_hash.get(0).split("#")[1], Integer.parseInt(ser_port_hash.get(0).split("#")[0]));
					DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
					if(((mapperCount == Slave.getMapperFinishedCount()) || mapperCount == 0) && already_sent_mapper == 0)
					{
						outToServer.writeBytes("SENDING_MAPPER_STATUS:"+ "1"  + ":" + allMapperFileCount +"\n");
						already_sent_mapper = 1;
					}
					else
						outToServer.writeBytes("SENDING_MAPPER_STATUS:"+ "0" + ":" + "0" +"\n");
					outToServer.flush();
					outToServer.close();
					clientSocket.close();
				}

				if(line.startsWith("GET_REDUCER_STATUS"))
				{
					/*
					 *  Master is requesting for reducer Status
					 *  Returns 1, if the reducer phase is done in this machine else 0
					 *  To Check if reducer phase is done, it checks,
					 *  number of reducers received and the number of reducers finished execution.
					 */
					Socket clientSocket = new Socket(ser_port_hash.get(0).split("#")[1], Integer.parseInt(ser_port_hash.get(0).split("#")[0]));
					DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
					if(((reducerCount == Slave.getReducerFinishedCount()) || reducerCount == 0) && already_sent_reducer == 0)
					{
						outToServer.writeBytes("SENDING_REDUCER_STATUS:"+ "1" +"\n");
						already_sent_reducer = 1;
					}
					else
						outToServer.writeBytes("SENDING_REDUCER_STATUS:"+ "0" +"\n");
					outToServer.flush();
					outToServer.close();
					clientSocket.close();
				}

				if(line.startsWith("KILL_YOURSELF")){
					// Kill the machine
					System.out.println("Killing myself.....");
					Thread.sleep(5000);
					s.close();
					serverSock.close();
					System.exit(0);
				}

				if(line.startsWith("DO_REDUCE")){
					// Start Reducer Thread
					reducerCount = line.split("#").length;
					className = line.split(":")[2];
					ReducerThread robj = new ReducerThread(line.split(":")[1]);
					Thread rt = new Thread(robj);
					rt.start();
				}

			}

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}

}

/*
 *  Mapper Thread
 *  This thread starts n threads.
 *  n - number of mappers
 */
class MapperThread extends Thread{

	public static AmazonS3 s3Client = new AmazonS3Client(Slave.clientConfig);
	String mapInfo;
	public static List<String> bucketKeys = new ArrayList<String>();

	public MapperThread(String mapInfo)
	{
		this.mapInfo = mapInfo;
	}

	public void run(){

		/* Get Keys 
		 * Get all the Files in input folder
		 * */
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(Slave.inBucket)
				.withPrefix(Slave.inPrefix);
		ObjectListing objectListing;     
		// Get list of all files from bucket and store them in a ArrayList
		do {
			objectListing = s3Client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : 
				objectListing.getObjectSummaries()) {
				String key=objectSummary.getKey();
				if(key.equals(Slave.inPrefix))
					continue;
				bucketKeys.add(key);
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());

		/*
		 *  Start all the mapper threads
		 *  Each mapper thread has its own Context object
		 */
		String[] listMapIndex = mapInfo.split("#");
		int mCount = 0;
		for(String mapIndex : listMapIndex)
		{
			Context ctx = new Context(mCount, Context.MAPPER_TYPE, new Text(""));
			MapperTask mtask = new MapperTask(mapIndex.split(",")[0], mapIndex.split(",")[1], mapIndex.split(",")[2], ctx);
			Thread mtT = new Thread(mtask);
			mtT.start();

			mCount++;
		}
	}
}

/*
 *  Custom Mapper Method
 *  Each Mapper has the index list of files it has to read.
 */

class MapperTask extends Thread{

	Context ctx = null;
	BufferedReader reader = null;
	int startIndex;
	int endIndex;
	int mapperNumber;

	public MapperTask(String startIndex, String endIndex, String mapperNumber, Context ctx)
	{
		this.startIndex = Integer.parseInt(startIndex);
		this.endIndex = Integer.parseInt(endIndex);
		this.mapperNumber = Integer.parseInt(mapperNumber);
		this.ctx = ctx;
	}

	public void run()
	{
		/*
		 * Use Java Reflection to get map method of respective class
		 * The class name is passed as an argument
		 */
		Class<?> c = null;
		Method method = null;
		try 
		{
			// Get map method of the Class
			c = Class.forName(Slave.className+"$M");
			method = c.getMethod("map",new Class[] { LongWritable.class, Text.class, Context.class});
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}

		/* Read from each key(file) that this mapper is supposed to read */
		for(int i=startIndex;i<=endIndex;i++){
			S3Object s3object = MapperThread.s3Client.getObject(new GetObjectRequest(
					Slave.inBucket, MapperThread.bucketKeys.get(i)));

			// Get a range of bytes from an object.
			GetObjectRequest rangeObjectRequest = new GetObjectRequest(
					Slave.inBucket, MapperThread.bucketKeys.get(i));
			rangeObjectRequest.setRange(0, 10);

			InputStreamReader decoder = null;
			// If the file is of text format
			if(s3object.getObjectMetadata().getContentType().contains("text"))
				decoder = new InputStreamReader(s3object.getObjectContent());
			else
			{
				GZIPInputStream gzin = null;
				try {
					gzin = new GZIPInputStream(s3object.getObjectContent());
				} catch (IOException e) {
					e.printStackTrace();
				}
				decoder = new InputStreamReader(gzin);
			}
			reader = new BufferedReader(decoder);
			String readline ="";
			try {
				while((readline = reader.readLine()) != null)
				{
					try {
						// Read each line and pass it to the respective map method (Invoking)
						method.invoke(c.newInstance(), new LongWritable(""), new Text(readline), ctx);
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					} catch (InvocationTargetException e) {
						e.printStackTrace();
					} catch (InstantiationException e) {
						e.printStackTrace();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// Finally write it to local disk
		try {
			ctx.writeToDisk(mapperNumber);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Increment mapperFinishedCount variable
		Slave.incMapperFinishedCount();
	}

}

/*
 * Reducer Thread
 * This thread starts n threads.
 * n - number of reducers
 */

class ReducerThread extends Thread{

	public static AmazonS3 s3RedcerClient = new AmazonS3Client(Slave.clientConfig);
	public static HashMap<String, ArrayList<String>> reducerKeyMap = new HashMap<String, ArrayList<String>>();
	String reducer_info;


	public ReducerThread(String message){
		this.reducer_info = message;
	}

	public void run(){
		/* Get Keys */
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(Slave.interBucket)
				.withPrefix(Slave.interPrefix);
		ObjectListing objectListing;     
		/*
		 * Get list of all files from bucket and store them in a HashMap
		 * This HashMap contains the files each reducer has to read.
		 */
		do {
			objectListing = s3RedcerClient.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : 
				objectListing.getObjectSummaries()) {
				String key=objectSummary.getKey();
				if(key.equals(Slave.interPrefix))
					continue;
				String key_stripped = key.split("_")[1];
				if(!reducerKeyMap.containsKey(key_stripped))
				{
					ArrayList<String> rKeys = new ArrayList<String>();
					rKeys.add(key);
					reducerKeyMap.put(key_stripped, rKeys);
				}
				else
				{
					ArrayList<String> erKeys = reducerKeyMap.get(key_stripped);
					erKeys.add(key);
					reducerKeyMap.put(key_stripped, erKeys);
				}
			}
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());

		// Start all the reducer threads
		String[] listReducerIndex = reducer_info.split("#");

		int rCount = 0;
		// Start n Reducers
		for(String reducerIndex : listReducerIndex)
		{
			Context rctx = new Context(rCount, Context.REDUCER_TYPE, new Text(reducerIndex.split("@")[0]));
			ReducerTask rtask = new ReducerTask(reducerIndex.split("@")[0], reducerIndex.split("@")[1], reducerKeyMap.get(reducerIndex.split("@")[0]), rctx);
			Thread rtT = new Thread(rtask);
			rtT.start();
			rCount++;
		}

	}

}


/*
 * Each reducer
 */

class ReducerTask extends Thread{

	BufferedReader reader = null;
	List<Text> values = null;
	Iterable<Text> final_values = null;
	String key;
	int rNumber;
	ArrayList<String> fileList = null;
	Context context;


	public ReducerTask(String key, String rNumber, ArrayList<String> fileList, Context ctx){
		this.key = key;
		this.values = new ArrayList<Text>();
		this.rNumber = Integer.parseInt(rNumber);
		this.fileList = fileList;
		this.context = ctx;

	}
	public void run(){

		/*
		 * Using Java Reflections to invoke reduce method of the respective Class.
		 */
		Class<?> c = null;
		Method method = null;
		try {
			// Get Reduce method of the class
			c = Class.forName(Slave.className+"$R");
			method = c.getMethod("reduce",new Class[] { Text.class, Iterable.class, Context.class});
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}

		/* Read from each key(file) that this reducer is supposed to read */
		for(int i=0; i< fileList.size(); i++){
			S3Object s3object = ReducerThread.s3RedcerClient.getObject(new GetObjectRequest(
					Slave.interBucket, fileList.get(i)));

			// Get a range of bytes from an object.
			GetObjectRequest rangeObjectRequest = new GetObjectRequest(
					Slave.interBucket, fileList.get(i));
			rangeObjectRequest.setRange(0, 10);

			InputStreamReader decoder = new InputStreamReader(s3object.getObjectContent());
			reader = new BufferedReader(decoder);
			String readline;
			try {
				while((readline = reader.readLine()) != null)
				{
					Text val = new Text(readline);
					values.add(val);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				s3object.close();
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {
			/*
			 * Invoke reduce method of respective Class.
			 * Pass the list(Iterable) of values to the reduce method
			 */
			Text[] val= new Text[values.size()];
			for(int i = 0; i < values.size(); i++)
				val[i] = values.get(i);
			final_values = Arrays.asList(val);
			method.invoke(c.newInstance(), new Text(key), final_values, context);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		}

		// Finally write the file to local directory
		try {
			context.writeToDisk(rNumber);
		} catch (IOException e) {
			e.printStackTrace();
		}

		// Increment reducerFinishedCount variable
		Slave.incReducerFinishedCount();
	}

}
