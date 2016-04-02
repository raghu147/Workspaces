import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.zip.GZIPInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;



public class Server extends Thread{

	int port;
	int serverNumber;
	int totalServers;
	String bucketName;
	String outputBucketName; 
	int monitorPort;
	static int server_stop = 0;
	static String rangeForCurrentServer;
	static HashMap<String, Double> tempListMap;
	static HashMap<String, Double> tempListFromOtherServerMap;
	static HashMap<String, Double> finalTempListMap;
	static HashMap<String, Double> topTempListMap;
	static Collection<String> topTenTempList;
	static HashMap<Integer, String> rangeMap = new HashMap<Integer, String>();
	static HashMap<Integer, String> ser_port_hash = new HashMap<Integer, String>();
	static String incomingRange;
	static String allRanges[];
	PrintWriter writer = null;

	public Server(int p, int serverNumber, int totalServers, String bucketName,String outputBucketName, int monitorPort){
		this.port = p;
		this.serverNumber = serverNumber;
		this.totalServers = totalServers;
		this.bucketName = bucketName;
		this.outputBucketName = outputBucketName;
		this.monitorPort = monitorPort;
		tempListMap = new HashMap<String, Double>();
		tempListFromOtherServerMap = new HashMap<String, Double>();
		finalTempListMap = new HashMap<String, Double>();
		topTempListMap = new HashMap<String, Double>();
		topTenTempList = new ArrayList<String>();
	}

	@Override
	public void run() {

		ServerSocket serverSock = null;
		Socket s = null;
		try {
			writer = new PrintWriter(this.serverNumber+"topten.txt","UTF-8");
		} catch (Exception e1) {
			e1.printStackTrace();
		}

		try {
			serverSock = new ServerSocket(port);
			System.out.println("Server listening at port:"+port);

			while(true)
			{
				s = serverSock.accept();
				BufferedReader inFromClient = new BufferedReader(new InputStreamReader(s.getInputStream()));
				String line = inFromClient.readLine();

				// Listening to all the port numbers
				if(line.startsWith("SERVER_PORT_DNS_LIST")){
					String s_p_line = line.split(":")[1];
					String server_port_list[] = s_p_line.split(",");
					for(String sp : server_port_list)
					{
						String port_dns = sp.split("=")[1];
						int server_num = Integer.parseInt(sp.split("=")[0]);
						ser_port_hash.put(server_num, port_dns);
					}	
				}
				// Listening to all the ranges broadcasted
				if(line.startsWith("RANGE"))
				{
					incomingRange = line.split(":")[1];
					allRanges = incomingRange.split(",");
					for(String r: allRanges)
					{
						int ser_num = Integer.parseInt(r.split("#")[0]);
						String rstring = r.split("#")[1];
						if(ser_num == serverNumber)
							rangeForCurrentServer = r.split("#")[1];
						rangeMap.put(ser_num, rstring);
					}
				}
				// Listening to other servers for temperature values
				if(line.startsWith("SEND_TEMP_VALUE"))
				{
					tempListFromOtherServerMap.put(line.split(":")[2], Double.parseDouble(line.split(":")[1]));
				}
				// Listening to the finished sending message
				if(line.startsWith("FIN_SENDING_DATA")){
					Thread readThread = new Thread(new ReadServer(port,serverNumber, totalServers, bucketName));
					readThread.start();
				}
				// Listening to Get Status message sent by master thread
				if(line.startsWith("GET_STATUS"))
				{
					Thread.sleep(10 * serverNumber);
					Socket clientSocket = new Socket(ser_port_hash.get(0).split("#")[1], monitorPort);
					DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
					outToServer.writeBytes("SENDING_STATUS:"+ server_stop + ":" + serverNumber +"\n");
					outToServer.flush();
					outToServer.close();
					clientSocket.close();
				}
				// Break the loop
				if(line.startsWith("KILL_YOURSELF"))
					break;
			}

			// Print top ten temperature values into a file
			Server.finalTempListMap.putAll(Server.tempListMap);
			Server.finalTempListMap.putAll(Server.tempListFromOtherServerMap);
			Server.topTempListMap = (HashMap<String, Double>) MapUtil.sortByValue(Server.finalTempListMap);

			topTenTempList = topTempListMap.keySet();
			Iterator<String> i = topTenTempList.iterator();
			int c = 0;
			while(i.hasNext() && c<10)
			{
				writer.println(i.next()+"\n");
				c++;
			}
			writer.close();
			s.close();

		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}

	}

}

class ReadServer extends Thread{

	static AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());

	int port;
	int serverNumber;
	int totalServers;
	String bucketName;
	static boolean currentServer;
	static int TOTAL_FILES = 133;
	static int numberOfFiles;
	static int startFileIndex;
	static int endFileIndex;
	static int factor;
	static int readComplete = 0;
	static List<String> bucketKeys = new ArrayList<String>();


	ReadServer(int p, int serverNumber, int totalServers, String bucketName)
	{
		this.port = p;
		this.serverNumber = serverNumber;
		this.totalServers = totalServers;
		this.bucketName = bucketName;
	}
	@Override
	public void run(){
		try {
			BufferedReader reader = null;
			System.out.println("Downloading an object");

			// Calculating the file indexes that each server has to read
			factor = TOTAL_FILES / totalServers;
			startFileIndex = (serverNumber * factor);
			endFileIndex = (serverNumber * factor) + factor - 1;
			if((TOTAL_FILES - endFileIndex) < factor)
				endFileIndex = TOTAL_FILES - 1;


			/* Get Keys */
			ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
					.withBucketName(bucketName)
					.withPrefix("climate");
			ObjectListing objectListing;     
			// Get list of all files from bucket and store them in a ArrayList
			do {
				objectListing = s3Client.listObjects(listObjectsRequest);
				for (S3ObjectSummary objectSummary : 
					objectListing.getObjectSummaries()) {
					String key=objectSummary.getKey();
					if(key.equals("climate/"))
						continue;
					bucketKeys.add(key);
				}
				listObjectsRequest.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());


			/* Read from each key(file) that this server is supposed to read */
			for(int i=startFileIndex;i<=startFileIndex;i++){
				System.out.println(bucketKeys.get(i)+"FILENAME");
				S3Object s3object = s3Client.getObject(new GetObjectRequest(
						bucketName, bucketKeys.get(i)));
				System.out.println("Content-Type: "  + 
						s3object.getObjectMetadata().getContentType());

				// Get a range of bytes from an object.
				GetObjectRequest rangeObjectRequest = new GetObjectRequest(
						bucketName, bucketKeys.get(i));
				rangeObjectRequest.setRange(0, 10);

				System.out.println("Printing bytes retrieved.");
				System.out.println(s3object.getObjectContent()+"filename");

				GZIPInputStream gzin = new GZIPInputStream(s3object.getObjectContent());
				InputStreamReader decoder = new InputStreamReader(gzin);
				reader = new BufferedReader(decoder);
				String readline;
				// Calculate min and max range of current server
				double start = Double.parseDouble(Server.rangeForCurrentServer.split("=")[0]);
				double end =  Double.parseDouble(Server.rangeForCurrentServer.split("=")[1]);

				while((readline = reader.readLine()) != null) {
					if(!readline.startsWith("Wban Number")){

						String splits[]=readline.split(",");
						if(splits.length>=8){
							String dryBulbTemp=splits[8];
							String wban = splits[0];
							String date = splits[1];
							String time = splits[2];
							// Basic sanity
							if(!dryBulbTemp.isEmpty()&& isDouble(dryBulbTemp)){
								Double temperature = Double.parseDouble(dryBulbTemp);
								String value = wban + "," +date +","+ time +","+ temperature;
								// If its within its range
								if(temperature >= start && temperature <= end)
									Server.tempListMap.put(value, temperature);
								// If it belongs to some other server
								else
									sendToOtherServer(temperature, value);
							}
						}
					}
				}
				s3object.close();
			}

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which" +
					" means your request made it " +
					"to Amazon S3, but was rejected with an error response" +
					" for some reason.");
			System.out.println("Error Message:    " + ase.getMessage());
			System.out.println("HTTP Status Code: " + ase.getStatusCode());
			System.out.println("AWS Error Code:   " + ase.getErrorCode());
			System.out.println("Error Type:       " + ase.getErrorType());
			System.out.println("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means"+
					" the client encountered " +
					"an internal error while trying to " +
					"communicate with S3, " +
					"such as not being able to access the network.");
			System.out.println("Error Message: " + ace.getMessage());
		} catch(Exception e){
			e.printStackTrace();
		}

		System.out.println("Read is done.......");
		Server.server_stop = 1;
	}

	// To check if value is double
	public boolean isDouble(String dryBulbTemp) {
		// TODO Auto-generated method stub
		try {
			Double.parseDouble(dryBulbTemp);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}
	// Sending the value to other servers
	public void sendToOtherServer(double temp, String value) throws UnknownHostException, IOException
	{
		for(int s : Server.rangeMap.keySet())
		{
			String r_for_s = Server.rangeMap.get(s);
			double start_for_s = Double.parseDouble(r_for_s.split("=")[0]);
			double end_for_s = Double.parseDouble(r_for_s.split("=")[1]);
			if((start_for_s <= temp) && (temp <= end_for_s))
			{
				if(Server.ser_port_hash.containsKey(s)){
					try{
						Socket clientSocket = new Socket(Server.ser_port_hash.get(s).split("#")[1], Integer.parseInt(Server.ser_port_hash.get(s).split("#")[0]));
						DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
						outToServer.writeBytes("SEND_TEMP_VALUE:"+temp +":"+value+ "\n");
						outToServer.flush();
						outToServer.close();
						clientSocket.close();
					}
					catch(Exception e)
					{
						continue;
					}
				}		
			}
		}
	}
}

// StackOverflow - To sort a hash map
class MapUtil
{
	public static <K, V extends Comparable<? super V>> Map<K, V> 
	sortByValue( Map<K, V> map )
	{
		List<Map.Entry<K, V>> list =
				new LinkedList<Map.Entry<K, V>>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<K, V>>()
		{
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
			{
				return (o2.getValue()).compareTo( o1.getValue() );
			}
		} );

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list)
		{
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}
}

