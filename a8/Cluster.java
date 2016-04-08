import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
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
	static AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
	static List<String> sampleKeys = new ArrayList<String>();
	static BufferedReader reader = null;
	static BufferedReader dnsreader = null;
	static Set<Double> sampleTempList = new HashSet<Double>();
	public static void main(String arg[]) throws UnknownHostException, IOException{

		// Reading all arguments
		String range="";
		String server_port_dns="";
		String dns_file_path = "publicDNS.txt";

		// 4 0 cs6240sp16 s3output 7777(monitor port) 11000 12000 13000 14000 
		int totalServers = Integer.parseInt(arg[0]);
		int serverNumber = Integer.parseInt(arg[1]);
		String bucketName = arg[2];
		String outputBucketName = arg[3];
		int monitorPort = Integer.parseInt(arg[4]);

		List<Integer> serverPortList = new ArrayList<Integer>();
		String[] serverDNSList = null;

		// Read the DNS file, and store it in a string(contains dns of all machines)
		try(BufferedReader br = new BufferedReader(new FileReader(dns_file_path))){
			String line;
			while ((line = br.readLine()) != null) {
				serverDNSList = line.split(" ");
			}
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		// Copy all the ports into an ArrayList
		for(int i = 5; i <  arg.length;i++){
			serverPortList.add(Integer.parseInt(arg[i]));
		}
		// Copy a combination of server number, server port and server dns into a string
		// 0=localhost#8890,1=localhost#8891
		for(int i = 0; i < totalServers; i++)
		{
			server_port_dns = server_port_dns + i + "=" + serverPortList.get(i) + "#"+serverDNSList[i] + ",";
		}
		server_port_dns = server_port_dns.substring(0, server_port_dns.length() - 1);

		// Start Server thread
		Thread t1 = new Thread(new Server(serverPortList.get(serverNumber),serverNumber,totalServers, bucketName, outputBucketName, monitorPort));	

		t1.start();

		/* Pass Ranges */
		try{
			// If server is master, sort the temperatures read from bucket to calculate ranges
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

				// Calculate ranges
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

				// Broadcast the values Server_Port_DNS, Range
				for(int i = 0;i  <  serverPortList.size(); i++)
					dispatchSendMessage(serverPortList.get(i), serverDNSList[i], "SERVER_PORT_DNS_LIST:"+server_port_dns);
				for(int i = 0;i  <  serverPortList.size(); i++)
					dispatchSendMessage(serverPortList.get(i), serverDNSList[i], "RANGE:"+range);
				for(int i = 0;i  <  serverPortList.size(); i++)
					dispatchSendMessage(serverPortList.get(i), serverDNSList[i],"FIN_SENDING_DATA:");

				ServerSocket serverSock = null;
				Socket s = null;

				// Check the status of all the machines and kills them, once all the machines finish reading
				try {
					int count = 0;
					List<Integer> servers_ended = new ArrayList<Integer>();
					serverSock = new ServerSocket(7777);

					while(true)
					{
						for(int i = 0;i  <  serverPortList.size(); i++)
						{
							if(!servers_ended.contains(i))
							dispatchSendMessage(serverPortList.get(i), serverDNSList[i],"GET_STATUS:");
						}
						s = serverSock.accept();
						BufferedReader inFromClient = new BufferedReader(new InputStreamReader(s.getInputStream()));
						String line = inFromClient.readLine();

						if(line.startsWith("SENDING_STATUS"))
						{
							// Receives the status
							//System.out.println("Count value, server, stop  ...." + count +"," + Integer.parseInt(line.split(":")[2])+"," + Integer.parseInt(line.split(":")[1]));
							if((!servers_ended.contains(Integer.parseInt(line.split(":")[2]))) && (Integer.parseInt(line.split(":")[1]) == 1))
							{
								count += Integer.parseInt(line.split(":")[1]);
								//System.out.println("Count value incremented...." + count);
								servers_ended.add(Integer.parseInt(line.split(":")[2]));
							}
						}
						if(count == totalServers)
						{
							// Kill machines
							for(int i = 0;i  <  serverPortList.size(); i++)
								dispatchSendMessage(serverPortList.get(i), serverDNSList[i],"KILL_YOURSELF:");
							break;
						}
						Thread.sleep(11000);
					}
					s.close();
				}
				catch(Exception e){
					e.printStackTrace();
				}

			}
		}catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	// Check if value is double
	private static boolean isDouble(String dryBulbTemp) {
		// TODO Auto-generated method stub
		try {
			Double.parseDouble(dryBulbTemp);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	// Dispatching messages to respective hostname and port
	public static void dispatchSendMessage(int port, String dns, String message) throws UnknownHostException, IOException{

		Client obj = new Client(port,dns, message+"\n");
		Thread sendThread = new Thread(obj);
		sendThread.run();
		obj.send();
	}


}

class Client extends Thread{

	int port;
	String message;
	String dns;
	ArrayList<String> range;

	public Client(int sendToPort, String dns, String message) {

		this.port = sendToPort;
		this.dns = dns;
		this.message = message;
	}

	public void send()throws UnknownHostException, IOException {
		Socket clientSocket = new Socket(dns,port);
		DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
		outToServer.writeBytes(message + '\n');
		outToServer.flush();
		outToServer.close();
		clientSocket.close(); 

	}

}
