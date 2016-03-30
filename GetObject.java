import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;


public class GetObject {
	private static String bucketName = "cs6240sp16"; 
	private static String key        = "climate/199607hourly.txt.gz";      

	public static void main(String[] args) throws IOException {
		AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
		// 4 0 11000 12000 13000 14000 
		int totalServers = Integer.parseInt(args[0]);
		int serverNumber = Integer.parseInt(args[1]);
		List<Integer> serverPortList = new ArrayList<Integer>();

		for(int i = 2; i <  args.length;i++){
			serverPortList.add(Integer.parseInt(args[i]));
		}
		try {
			System.out.println("Downloading an object");
			S3Object s3object = s3Client.getObject(new GetObjectRequest(
					bucketName, key));
			System.out.println("Content-Type: "  + 
					s3object.getObjectMetadata().getContentType());


			// Get a range of bytes from an object.

			GetObjectRequest rangeObjectRequest = new GetObjectRequest(
					bucketName, key);
			rangeObjectRequest.setRange(0, 10);
			S3Object objectPortion = s3Client.getObject(rangeObjectRequest);

			System.out.println("Printing bytes retrieved.");
			displayTextInputStream(s3object.getObjectContent(),totalServers,serverNumber,serverPortList);

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



	public static void displayTextInputStream(InputStream input, int totalServers, int serverNumber, List<Integer> serverPortList)
			throws IOException {
		Thread t1 = new Thread(new Server(serverPortList.get(serverNumber)));	

		t1.start();
		// Det Range
		List<String> rangeString = new ArrayList<String>();


		if(serverNumber == 0){

			// Input file
			// loop
			rangeString.add("0.0,50.0");
			rangeString.add("51.0,100.0");

			BufferedReader reader = null;


			try {
				// Read one text line at a time and display.
				GZIPInputStream gzin = new GZIPInputStream(input);
				InputStreamReader decoder = new InputStreamReader(gzin);
				reader = new BufferedReader(decoder);
				String line;

				while((line = reader.readLine()) != null) {
					if(!line.startsWith("Wban Number")){
						System.out.println(line);
						String splits[]=line.split(",");
						String dryBulbTemp=splits[8];

						//int temperature = Integer.parseInt(dryBulbTemp);
						if(!dryBulbTemp.isEmpty()&& !dryBulbTemp.equals("-")){
							Double temperature = Double.parseDouble(dryBulbTemp);
							//System.out.print(temperature);
							int server = 0;
							for(String r : rangeString){
								//System.out.println("insidefor");
								double start = Double.parseDouble(r.split(",")[0]);
								double end =  Double.parseDouble(r.split(",")[1]);

								if(temperature > start && temperature < end){

									int sendToPort = serverPortList.get(server);
									dispatchSendMessage(sendToPort,"FROM_SERVER:"+temperature);
									//System.out.println("hello");
									break;
								}

								server ++;
							}

						}
					}
				}
				for(int i = 0;i  <  serverPortList.size(); i++)
					dispatchSendMessage(serverPortList.get(i),"FIN_SENDING_DATA:");


			}   
			catch(Exception e){

			}
			reader.close();

		}

	}



	private static void dispatchSendMessage(Integer port, String message) throws UnknownHostException, IOException {
		// TODO Auto-generated method stub
		Client obj = new Client(port,message+"\n");
		Thread sendThread = new Thread(obj);
		sendThread.run();
		obj.send();
	}

}


class Client extends Thread{

	int port;
	String message;

	public Client(int sendToPort, String message) {

		this.port = sendToPort;
		this.message = message;
	}


	public void send()throws UnknownHostException, IOException {

		Socket clientSocket = new Socket("localhost",port);
		DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
		outToServer.writeBytes(message + '\n');
		clientSocket.close(); 

	}
}
