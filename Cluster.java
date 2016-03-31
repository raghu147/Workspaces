import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

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


public class Cluster {
	private static String bucketName = "cs6240sp16";
	static AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
	static List<String> sampleKeys = new ArrayList<String>();
	static BufferedReader reader = null;
	static Set<Double> sampleTempList = new HashSet<Double>();
	public static void main(String arg[]) throws UnknownHostException, IOException{

		String range="";
		String server_port="";


		// 4 0 11000 12000 13000 14000 
		int totalServers = Integer.parseInt(arg[0]);
		int serverNumber = Integer.parseInt(arg[1]);

		List<Integer> serverPortList = new ArrayList<Integer>();

		for(int i = 2; i <  arg.length;i++){
			serverPortList.add(Integer.parseInt(arg[i]));
		}
		for(int i = 0; i < totalServers; i++)
		{
			server_port = server_port + i + "-" + serverPortList.get(i) + ",";
		}
		server_port = server_port.substring(0, server_port.length() - 1);

		Thread t1 = new Thread(new Server(serverPortList.get(serverNumber),serverNumber,totalServers));	

		t1.start();

		/* Pass Ranges */
		try{

			if(serverNumber == 0){
				/* Determine Ranges */
				try {
					
					/* Get Keys */
					ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
							.withBucketName(bucketName)
							.withPrefix("climate");
					ObjectListing objectListing;     

					do {
						objectListing = s3Client.listObjects(listObjectsRequest);
						for (S3ObjectSummary objectSummary : 
							objectListing.getObjectSummaries()) {
							String key=objectSummary.getKey();
							if(key.equals("climate/"))
								continue;
							if(key.equals("climate/200001hourly.txt.gz")
									|| key.equals("climate/200102hourly.txt.gz")
									|| key.equals("climate/200203hourly.txt.gz")
									|| key.equals("climate/200304hourly.txt.gz")
									|| key.equals("climate/200405hourly.txt.gz")
									|| key.equals("climate/200506hourly.txt.gz")
									|| key.equals("climate/200607hourly.txt.gz")
									|| key.equals("climate/199608hourly.txt.gz")
									|| key.equals("climate/199609hourly.txt.gz")
									|| key.equals("climate/199710hourly.txt.gz")
									|| key.equals("climate/199811hourly.txt.gz")
									|| key.equals("climate/200012hourly.txt.gz") ){
								sampleKeys.add(key);
							} 

							//sampleKeys.add(key);
						}
						listObjectsRequest.setMarker(objectListing.getNextMarker());
					} while (objectListing.isTruncated());

					/* Read from each key */
					for(int i=0;i<sampleKeys.size();i++){

						S3Object s3object = s3Client.getObject(new GetObjectRequest(
								bucketName, sampleKeys.get(i)));
						
						// Get a range of bytes from an object.

						GetObjectRequest rangeObjectRequest = new GetObjectRequest(
								bucketName, sampleKeys.get(i));
						rangeObjectRequest.setRange(0, 10);
						
						GZIPInputStream gzin = new GZIPInputStream(s3object.getObjectContent());
						InputStreamReader decoder = new InputStreamReader(gzin);
						reader = new BufferedReader(decoder);
						String line;

						while((line = reader.readLine()) != null) {

							if(!line.startsWith("Wban Number")){
								
								String splits[]=line.split(",");
								if(splits.length>=8){
									String dryBulbTemp=splits[8];
									
									if(!dryBulbTemp.isEmpty()&& isDouble(dryBulbTemp)){
										Double temperature = Double.parseDouble(dryBulbTemp);
										sampleTempList.add(temperature);
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
				}

				List<Double> sampleList = new ArrayList<Double>(sampleTempList);
				Collections.sort(sampleList);


				int totalLength = sampleList.size();
				int factor = totalLength/totalServers;

				int startIndex=0;
				int endIndex=0;


				for(int i=0;i<totalServers;i++){
					if(i==0){
						startIndex= factor*i;
					}
					else{
						startIndex=factor*i+1;

					}
					endIndex=factor*i+factor;

					//range = "0#0.0=50.0,1#51.0=100.0";
					range = range+i+"#"+sampleList.get(startIndex)+"="+sampleList.get(endIndex)+",";
				}
				range=range.substring(0,range.length()-1);

			
				for(int port : serverPortList)
					dispatchSendMessage(port, "SERVER_PORT_LIST:"+server_port);
				for(int port : serverPortList)
				{
					dispatchSendMessage(port, "RANGE:"+range);
				}
				for(int i = 0;i  <  serverPortList.size(); i++)
					dispatchSendMessage(serverPortList.get(i),"FIN_SENDING_DATA:");
				 

			} 
		}catch(Exception e){

		}


	}

	private static boolean isDouble(String dryBulbTemp) {
		// TODO Auto-generated method stub
		try {
			Double.parseDouble(dryBulbTemp);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	public static void dispatchSendMessage(int port,String message) throws UnknownHostException, IOException{

		Client obj = new Client(port,message+"\n");
		Thread sendThread = new Thread(obj);
		sendThread.run();
		obj.send();
	}


}



class Client extends Thread{

	int port;
	String message;
	ArrayList<String> range;

	public Client(int sendToPort, String message) {

		this.port = sendToPort;
		this.message = message;
	}
	public Client(int sendToPort, List<String> range) {

		this.port = sendToPort;
		this.range = (ArrayList<String>) range;
	}


	public void send()throws UnknownHostException, IOException {

		Socket clientSocket = new Socket("localhost",port);
		DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
		outToServer.writeBytes(message + '\n');
		clientSocket.close(); 

	}

}