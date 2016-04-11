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
import java.util.HashMap;
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

public class Master {
	static AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
	public static void main(String arg[]) throws UnknownHostException, IOException
	{
		// Arguments: totalMachines input output masterport port1 port2 ....
		int numOfMachines = Integer.parseInt(arg[0]);
		String inputBucketName = arg[1];
		String outputBucketName = arg[2];
		int masterPort = Integer.parseInt(arg[3]);

		String dns_file_path = "publicDNS.txt";
		List<Integer> serverPortList = new ArrayList<Integer>();
		String[] serverDNSList = null;
		String server_port_dns="";

		double totalSize = 0.0;
		int blockSize = 128;
		int totalNumOfMappers = 0;
		int factor = 0;
		int diff = 0;
		int file_count = 0;
		int startFileIndex = 0;
		int endFileIndex = 0;
		int file_factor = 0;
		HashMap<Integer, String> slaveInfo = new HashMap<Integer, String>();

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
		for(int i = 3; i <  arg.length;i++){
			serverPortList.add(Integer.parseInt(arg[i]));
		}

		// Copy a combination of server number, server port and server dns into a string
		// 0=localhost#8890,1=localhost#8891
		for(int i = 0; i <= numOfMachines; i++)
		{
			server_port_dns = server_port_dns + i + "=" + serverPortList.get(i) + "#"+serverDNSList[i] + ",";
		}
		server_port_dns = server_port_dns.substring(0, server_port_dns.length() - 1);

		// Read S3 bucket to calculate size and number of files
		ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(inputBucketName)
				.withPrefix("climate");
		ObjectListing objectListing;

		do {
			objectListing = s3Client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : 
				objectListing.getObjectSummaries()) {

				String key=objectSummary.getKey();
				if(key.equals("climate/"))
					continue;
				totalSize += objectSummary.getSize();
				file_count++;
			}
			totalSize /= 1024*1024;
			System.out.println(totalSize);
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());

		// Calculate number of mappers per machine
		totalNumOfMappers = (int)totalSize / blockSize;
		file_factor = file_count / totalNumOfMappers;
		factor = totalNumOfMappers / numOfMachines;
		diff = totalNumOfMappers - (factor * numOfMachines);

		// Store mapper per machine info in a hashmap
		startFileIndex = 0;
		endFileIndex = file_factor - 1;
		for(int i = 1; i<= numOfMachines;i++)
		{
			if(i == numOfMachines)
			{
				int mappers = (factor + diff);
				String map_info = "";
				for(int j = 1; j<=mappers; j++)
				{
					if(j == mappers)
						endFileIndex = file_count - 1;
					map_info += startFileIndex + "," + endFileIndex + "#";
					startFileIndex = endFileIndex + 1;
					endFileIndex += file_factor;
				}
				slaveInfo.put(i, map_info.substring(0,map_info.length()-1));
			}
			else
			{
				int mappers = factor;
				String map_info = "";
				for(int j=1; j<=mappers; j++)
				{
					map_info += startFileIndex + "," + endFileIndex + "#";
					startFileIndex = endFileIndex + 1;
					endFileIndex += file_factor;
				}
				slaveInfo.put(i, map_info.substring(0,map_info.length()-1));
			}
		}

		// Broadcast input, output bucket info to all slaves
		for(int i = 1;i  <  serverPortList.size(); i++)
			dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "BUCKET_INFO:"+inputBucketName+","+outputBucketName);
		// Broadcast server_port_dns info to all slaves
		for(int i = 1;i  <  serverPortList.size(); i++)
			dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "SERVER_PORT_DNS_LIST:"+server_port_dns);
		// Broadcast slave info to all slaves
		for(int i = 1;i  <  serverPortList.size(); i++)
			dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "MAPPER_INFO:"+slaveInfo.get(i));
		// Kill the slaves mapper phase
		for(int i = 1;i  <  serverPortList.size(); i++)
			dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "KILL_YOURSELF:");

		ServerSocket serverSock = null;
		Socket s = null;

		// Keep Listening to all the slaves to check if mappers finished their jobs
		try
		{
			serverSock = new ServerSocket(masterPort);
			System.out.println("Server listening at port: "+masterPort);
			while(true)
			{
				s = serverSock.accept();
				break;
			}
			s.close();
			serverSock.close();

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public static void dispatchSendMessage(String dns, int port, String message) throws UnknownHostException, IOException{
		Socket clientSocket = new Socket(dns, port);
		DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
		outToServer.writeBytes(message + '\n');
		outToServer.flush();
		outToServer.close();
		clientSocket.close(); 
	}

}
