import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.net.ServerSocket;
import java.net.Socket;
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
	
	
	
	//private static String key        = "climate/199607hourly.txt.gz";  
	AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
	
	/*try {
        System.out.println("Listing objects");

        ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
            .withBucketName(bucketName)
            .withPrefix("m");
        ObjectListing objectListing;            
        do {
            objectListing = s3Client.listObjects(listObjectsRequest);
            for (S3ObjectSummary objectSummary : 
            	objectListing.getObjectSummaries()) {
                System.out.println(" - " + objectSummary.getKey() + "  " +
                        "(size = " + objectSummary.getSize() + 
                        ")");
            }
            listObjectsRequest.setMarker(objectListing.getNextMarker());
        } while (objectListing.isTruncated());
     } catch (AmazonServiceException ase) {
        System.out.println("Caught an AmazonServiceException, " +
        		"which means your request made it " +
                "to Amazon S3, but was rejected with an error response " +
                "for some reason.");
        System.out.println("Error Message:    " + ase.getMessage());
        System.out.println("HTTP Status Code: " + ase.getStatusCode());
        System.out.println("AWS Error Code:   " + ase.getErrorCode());
        System.out.println("Error Type:       " + ase.getErrorType());
        System.out.println("Request ID:       " + ase.getRequestId());
    } catch (AmazonClientException ace) {
        System.out.println("Caught an AmazonClientException, " +
        		"which means the client encountered " +
                "an internal error while trying to communicate" +
                " with S3, " +
                "such as not being able to access the network.");
        System.out.println("Error Message: " + ace.getMessage());
    } */
	
	
	
	
	int port;
	int serverNumber;
	int numberOfFiles;
	boolean currentServer;
	//String inputFile = "data.txt";
	
	List<Double> tempList ;
	List<String> rangeString = new ArrayList<String>();
	List<String> bucketKeys = new ArrayList<String>();
	
	HashMap<Integer, Integer> ser_port_num = new HashMap<Integer, Integer>();
	String incomingRange;
	String allRanges[];
	String rangeForCurrentServer;
	
	
	public Server(int p, int serverNumber){
		this.port = p;
		this.serverNumber = serverNumber;
		tempList = new ArrayList<Double>();
	}

	@Override
	public void run() {
		
		ServerSocket serverSock = null;
		Socket s = null;
		BufferedReader reader = null;
		FileReader fileReader = null;
		
		
		try {
			serverSock = new ServerSocket(port);
			System.out.println("Server listening at port:"+port);
			
			while(true)
			{
				s = serverSock.accept();
				BufferedReader inFromClient =new BufferedReader(new InputStreamReader(s.getInputStream()));
				//ObjectInputStream objectInput = new ObjectInputStream(s.getInputStream());
				String line = inFromClient.readLine();
				
				/*Object object = objectInput.readObject();
				rangeString =  (ArrayList<String>) object;
                System.out.println(rangeString.get(1));
                //break;*/
				
				if(line.startsWith("SERVER_PORT_LIST")){
					String s_p_line = line.split(":")[1];
					String server_port_list[] = s_p_line.split(",");
					for(String sp : server_port_list)
					{
						int port_num = Integer.parseInt(sp.split("-")[1]);
						int server_num = Integer.parseInt(sp.split("-")[0]);
						System.out.println(port+","+port_num+","+server_num);
						if(port_num == port)
						{
							//System.out.println("Equal ports..");
							serverNumber = server_num;
						}
						ser_port_num.put(server_num, port_num);
					}
					//System.out.println("Server Number:" + serverNumber);
					
				}
				if(line.startsWith("RANGE"))
				{
					incomingRange = line.split(":")[1];
					//System.out.println("Range caught...." + line.split(":")[1]);
				}
					
				if(line.startsWith("FIN_SENDING_DATA")){
					break;
				}
				
			}
			
			allRanges = incomingRange.split(",");
			for(String r: allRanges)
			{
				int ser_num = Integer.parseInt(r.split("#")[0]);
				//System.out.println(ser_num);
				if(ser_num == serverNumber){
					rangeForCurrentServer = r.split("#")[1];
					currentServer = true;
				}
			}
			
			try
			{
				if(currentServer){
					try {
						System.out.println("Downloading an object");
						
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
					            	bucketKeys.add(key);
					            	//System.out.print(bucketKeys);
					                /*System.out.println(" - " + objectSummary.getKey() + "  " +
					                        "(size = " + objectSummary.getSize() + 
					                        ")"); */
					            }
					            listObjectsRequest.setMarker(objectListing.getNextMarker());
					        } while (objectListing.isTruncated());

						  
						  
						
						
						
						System.out.println(bucketKeys.size()+"BUCKETSIZE-----------");
						/* Read from each key */
						for(int i=0;i<bucketKeys.size();i++){
							System.out.println(bucketKeys.get(i)+"FILENAME");
						S3Object s3object = s3Client.getObject(new GetObjectRequest(
								bucketName, bucketKeys.get(i)));
						System.out.println("Content-Type: "  + 
								s3object.getObjectMetadata().getContentType());


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
						String line;
						double start = Double.parseDouble(rangeForCurrentServer.split("-")[0]);
						double end =  Double.parseDouble(rangeForCurrentServer.split("-")[1]);

						//System.out.println("Before while");
						while((line = reader.readLine()) != null) {
							
							if(!line.startsWith("Wban Number")){
							//System.out.println(line);
								
								String splits[]=line.split(",");
								if(splits.length>=8){
								String dryBulbTemp=splits[8];

								//int temperature = Integer.parseInt(dryBulbTemp);
								if(!dryBulbTemp.isEmpty()&& !dryBulbTemp.startsWith("-")){
									Double temperature = Double.parseDouble(dryBulbTemp);
									
								
										if(temperature > start && temperature < end){
											tempList.add(temperature);
										}

								}
								}
							}
							}
						s3object.close();
							//System.out.println("After while");
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
					
				
				}
			}
			catch(Exception e)
			{
				System.out.println(e);
			}
			
			Collections.sort(tempList);
			System.out.println("Server:"+serverNumber + " Sorted data");			
			for(Double d : tempList){
				System.out.println(d);
			}
			
			
			s.close();
			serverSock.close();
			
			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	 
}
