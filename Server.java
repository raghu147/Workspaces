import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;


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

	private static String bucketName = "cs6240sp16";
	AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());


	int port;
	int serverNumber;
	int numberOfFiles;
	boolean currentServer;
	int TOTAL_FILES = 133;
	int totalServers;
	int startFileIndex;
	int endFileIndex;
	int factor;
	int count = 1;

	List<Double> tempList ;
	List<Double> tempListFromOtherServer;
	HashMap<Integer, String> rangeMap = new HashMap<Integer, String>();
	List<String> bucketKeys = new ArrayList<String>();

	HashMap<Integer, Integer> ser_port_hash = new HashMap<Integer, Integer>();
	String incomingRange;
	String allRanges[];
	String rangeForCurrentServer;


	public Server(int p, int serverNumber, int totalServers){
		this.port = p;
		this.serverNumber = serverNumber;
		this.totalServers = totalServers;
		tempList = new ArrayList<Double>();
		tempListFromOtherServer = new ArrayList<Double>();
	}

	@Override
	public void run() {

		ServerSocket serverSock = null;
		Socket s = null;
		BufferedReader reader = null;

		// Calculating the file indexes that each server has to read
		factor = TOTAL_FILES / totalServers;
		startFileIndex = (serverNumber * factor);
		endFileIndex = (serverNumber * factor) + factor - 1;
		if((TOTAL_FILES - endFileIndex) < factor)
			endFileIndex = TOTAL_FILES - 1;

		try {
			serverSock = new ServerSocket(port);
			System.out.println("Server listening at port:"+port);

			while(true)
			{
				System.out.println("inside infinite while loop");
				s = serverSock.accept();
				BufferedReader inFromClient = new BufferedReader(new InputStreamReader(s.getInputStream()));
				String line = inFromClient.readLine();

				// Listening to all the port numbers
				if(line.startsWith("SERVER_PORT_LIST")){
					String s_p_line = line.split(":")[1];
					String server_port_list[] = s_p_line.split(",");
					for(String sp : server_port_list)
					{
						int port_num = Integer.parseInt(sp.split("-")[1]);
						int server_num = Integer.parseInt(sp.split("-")[0]);
						ser_port_hash.put(server_num, port_num);
					}	
				}
				// Listening to all the ranges broadcasted
				if(line.startsWith("RANGE"))
				{
					incomingRange = line.split(":")[1];
				}
				// Listening to other servers for temperature values
				if(line.startsWith("SEND_TEMP_VALUE"))
				{
					System.out.println("Received temp value from other server.....");
					tempListFromOtherServer.add(Double.parseDouble(line.split(":")[1]));
				}
				// Listening to the finished sending message 	
				if(line.startsWith("FIN_SENDING_DATA")){
					//break;
					// Calculate the range for the current server
					allRanges = incomingRange.split(",");
					for(String r: allRanges)
					{
						int ser_num = Integer.parseInt(r.split("#")[0]);
						String rstring = r.split("#")[1];
						if(ser_num == serverNumber)
							rangeForCurrentServer = r.split("#")[1];
						rangeMap.put(ser_num, rstring);
					}


					try {
						System.out.println("Downloading an object");

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

							System.out.println(count);
							count++;
							// Get a range of bytes from an object.

							GetObjectRequest rangeObjectRequest = new GetObjectRequest(
									bucketName, bucketKeys.get(i));
							rangeObjectRequest.setRange(0, 10);
							//S3Object objectPortion = s3Client.getObject(rangeObjectRequest);

							System.out.println("Printing bytes retrieved.");
							System.out.println(s3object.getObjectContent()+"filename");

							GZIPInputStream gzin = new GZIPInputStream(s3object.getObjectContent());
							InputStreamReader decoder = new InputStreamReader(gzin);
							reader = new BufferedReader(decoder);
							String readline;
							double start = Double.parseDouble(rangeForCurrentServer.split("-")[0]);
							double end =  Double.parseDouble(rangeForCurrentServer.split("-")[1]);
							System.out.println("Before while");
							while((readline = reader.readLine()) != null) {
								System.out.println("Inside while");
								if(!readline.startsWith("Wban Number")){

									String splits[]=readline.split(",");
									if(splits.length>=8){
										String dryBulbTemp=splits[8];

										if(!dryBulbTemp.isEmpty()&& !dryBulbTemp.startsWith("-")){
											Double temperature = Double.parseDouble(dryBulbTemp);
											if(temperature >= start && temperature <= end)
												tempList.add(temperature);
											else
												sendToOtherServer(temperature);
										}
									}
								}
							}
							System.out.println("After while");
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
						System.out.println(e);
					}

					tempList.addAll(tempListFromOtherServer);
					// Sorting all the temperature values
					Collections.sort(tempList);
					System.out.println("Server:"+serverNumber + " Sorted data");			
					for(Double d : tempList){
						System.out.println(d);
					}

					System.out.println("Length of tempList : " + tempList.size());
					System.out.println("Length of tempList : " + tempListFromOtherServer.size());

					s.close();
					serverSock.close();
					break;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void sendToOtherServer(double temp) throws UnknownHostException, IOException
	{
		for(int s : rangeMap.keySet())
		{
			String r_for_s = rangeMap.get(s);
			double start_for_s = Double.parseDouble(r_for_s.split("-")[0]);
			double end_for_s = Double.parseDouble(r_for_s.split("-")[1]);
			if((start_for_s <= temp) && (temp <= end_for_s))
			{
				if(ser_port_hash.containsKey(s)){
					dispatchMessageToServer(ser_port_hash.get(s), "SEND_TEMP_VALUE:" + temp);
					//dispatchMessageToServer(ser_port_hash.get(s), "FIN_SENDING_DATA:");
				}
				System.out.println("Sending to server: " + s + "with port "+ser_port_hash.get(s)+" and the range is: " +r_for_s);
			}
		}
	}

	public static void dispatchMessageToServer(int port, String message) throws UnknownHostException, IOException
	{
		System.out.println("In dispatch.....");
		ServerMessage obj = new ServerMessage(port,message+"\n");
		Thread sendThread = new Thread(obj);
		sendThread.run();
		obj.send();
	}
}

class ServerMessage extends Thread{

	int port;
	String message;

	public ServerMessage(int sendToPort, String message) {

		this.port = sendToPort;
		this.message = message;
	}

	public void send()throws UnknownHostException, IOException {
		System.out.println("In send method......");
		Socket clientSocket = new Socket("localhost",port);
		DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
		outToServer.writeBytes(message + '\n');
		clientSocket.close(); 

	}
}
