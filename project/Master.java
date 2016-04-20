import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class Master {
	static AmazonS3 s3Client = new AmazonS3Client(new ProfileCredentialsProvider());
	public static void main(String arg[]) throws UnknownHostException, IOException, InterruptedException
	{
		// Arguments: totalMachines input intermediate output masterport port1 port2 ....
		String classToExecute = arg[0];
		int numOfMachines = Integer.parseInt(arg[1]);
		String inputBucketName = arg[2];
		String intermediateBucketName = arg[3];
		String outputBucketName = arg[4];
		int masterPort = Integer.parseInt(arg[5]);

		String dns_file_path = "publicDNS.txt";
		List<Integer> serverPortList = new ArrayList<Integer>();
		String[] serverDNSList = null;
		String server_port_dns="";

		double totalSize = 0.0;
		//int blockSize = 128;
		int blockSize = 10;

		int totalNumOfMappers = 0;
		int factor = 0;
		int diff = 0;
		int file_count = 0;
		int startFileIndex = 0;
		int endFileIndex = 0;
		int file_factor = 0;
		int mappersToRead = 0;
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
		for(int i = 5; i <  arg.length;i++){
			serverPortList.add(Integer.parseInt(arg[i]));
		}

		// Copy a combination of server number, server port and server dns into a string
		// 0=localhost#8890,1=localhost#8891
		for(int i = 0; i <= numOfMachines; i++)
		{
			server_port_dns = server_port_dns + i + "=" + serverPortList.get(i) + "#"+serverDNSList[i] + ",";
		}
		server_port_dns = server_port_dns.substring(0, server_port_dns.length() - 1);


		//----------------------------------------------------------------------------------------
		//										MAPPER PHASE
		//----------------------------------------------------------------------------------------


		// Read S3 bucket to calculate size and number of files

		ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(inputBucketName);
		//.withPrefix("climate");
		ObjectListing objectListing;
		do {
			objectListing = s3Client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : 
				objectListing.getObjectSummaries()) {
				String key=objectSummary.getKey();
				if(key.equals("airline/"))
					continue;
				totalSize += objectSummary.getSize();
				file_count++;
			}
			totalSize /= 1024*1024;
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());

		// Calculate number of mappers per machine
		if(totalSize < blockSize)
			totalNumOfMappers = 1;
		else
			totalNumOfMappers = (int)totalSize / blockSize;

		file_factor = file_count / totalNumOfMappers;
		if(file_factor==0)
			totalNumOfMappers=file_count;

		factor = totalNumOfMappers / numOfMachines;
		diff = totalNumOfMappers - (factor * numOfMachines);
		// Store mapper per machine info in a hashmap
		startFileIndex = 0;
		if(file_factor==0)
			endFileIndex=0;
		else
			endFileIndex = file_factor - 1;
		int map_count = 1;
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
					map_info += startFileIndex + "," + endFileIndex + "," + map_count + "#";
					map_count ++;
					startFileIndex = endFileIndex + 1;
					if(file_factor==0)
						endFileIndex+=1;
					else
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
					map_info += startFileIndex + "," + endFileIndex + "," + map_count + "#";
					map_count ++;
					startFileIndex = endFileIndex + 1;
					if(file_factor==0)
						endFileIndex+=1;
					else
						endFileIndex += file_factor;
				}
				if(!map_info.isEmpty())
					slaveInfo.put(i, map_info.substring(0,map_info.length()-1));
			}
		}


		// Broadcast input, output bucket info to all slaves
		for(int i = 1;i  <  serverPortList.size(); i++)
			dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "BUCKET_INFO:"+inputBucketName+","+intermediateBucketName +","+outputBucketName);

		Thread.sleep(1000);

		// Broadcast server_port_dns info to all slaves
		for(int i = 1;i  <  serverPortList.size(); i++)
			dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "SERVER_PORT_DNS_LIST:"+server_port_dns);

		// Broadcast slave info to all slaves
		for(int i = 1;i  <  serverPortList.size(); i++)
		{
			if(slaveInfo.get(i) != null)
				dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "MAPPER_INFO:"+slaveInfo.get(i)+":"+classToExecute);
		}

		System.out.println("Started Mapper phase....");

		ServerSocket serverSock = null;
		Socket s = null;
		// Keep Listening to all the slaves to check if mappers finished their jobs
		try
		{
			int stop_count = 0;
			serverSock = new ServerSocket(masterPort);
			System.out.println("Server listening at port: "+masterPort);
			while(true)
			{
				for(int i = 1;i  <  serverPortList.size(); i++)
					dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "GET_MAPPER_STATUS:");
				s = serverSock.accept();
				BufferedReader inFromClient = new BufferedReader(new InputStreamReader(s.getInputStream()));
				String line = inFromClient.readLine();

				if(line.startsWith("SENDING_MAPPER_STATUS"))
				{
					stop_count += Integer.parseInt(line.split(":")[1]);
					mappersToRead += Integer.parseInt(line.split(":")[2]);
				}
				System.out.println(stop_count + "========= Stop Count");
				if(stop_count == numOfMachines)
				{
					// Mapper phase in all machines is done
					break;
				}
				Thread.sleep(11000);
			}
			System.out.println("Finished Mapper phase.....");
			s.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		System.out.println("Starting Intermediate phase...Shuffle and Sort...");

		//----------------------------------------------------------------------------------------
		//										INTERMEDIATE PHASE
		//----------------------------------------------------------------------------------------


		ArrayList<String> mrKeys = new ArrayList<String>();

		// Read intermediate S3 bucket to get mapper files and calculate number of keys

		ListObjectsRequest listObjectsRequestIntermediate = new ListObjectsRequest()
				.withBucketName(intermediateBucketName);
		ObjectListing objectListingIntermediate;
		int m_count;
		do {
			m_count = 0;
			do {
				objectListingIntermediate = s3Client.listObjects(listObjectsRequestIntermediate);
				for (S3ObjectSummary objectSummary : 
					objectListingIntermediate.getObjectSummaries()) {
					String key=objectSummary.getKey();
					String key_value = key.split("_")[1];
					m_count++;
					if(!mrKeys.contains(key_value))
						mrKeys.add(key_value);
				}
				listObjectsRequestIntermediate.setMarker(objectListingIntermediate.getNextMarker());
			} while (objectListingIntermediate.isTruncated());
			System.out.println("Intermediate NUMBER OF KEYS, COUNT, MappersToRead = " +mrKeys.size() +","+m_count+","+mappersToRead);
		}while(mappersToRead!=m_count);

		System.out.println("NUMBER OF KEYS = " +mrKeys.size());
		
		//----------------------------------------------------------------------------------------
		//										REDUCER PHASE
		//----------------------------------------------------------------------------------------
		System.out.println("Starting Reducer Phase.....");
		
		// Distribute all the keys among reducers present in different machines

		int reducersPerMachine = 0;
		int reducerDiff = 0;
		int reducer_number = 0;
		HashMap<Integer, String> slaveReducerInfo = new HashMap<Integer, String>();
		int allKeys = mrKeys.size();

		reducersPerMachine = allKeys/numOfMachines;
		if(reducersPerMachine == 0)
		{
			reducersPerMachine = 1;
			reducerDiff = 0;
		}
		else
			reducerDiff = allKeys - (reducersPerMachine * numOfMachines);

		for(int i = 1; i<= numOfMachines;i++)
		{
			if(reducer_number <= allKeys)
			{
				if(i == numOfMachines)
				{
					int reducers = (reducersPerMachine + reducerDiff);
					String reducer_info = "";
					for(int j = 1; j<=reducers; j++)
					{
						reducer_info += mrKeys.get(reducer_number) + "," + (reducer_number + 1) + "#";
						reducer_number ++;
					}
					slaveReducerInfo.put(i, reducer_info.substring(0,reducer_info.length()-1));
				}
				else
				{
					String reducer_info = "";
					for(int j=1; j<=reducersPerMachine; j++)
					{
						reducer_info += mrKeys.get(reducer_number) + "," + (reducer_number + 1) + "#";
						reducer_number ++;
					}
					if(!reducer_info.isEmpty())
						slaveReducerInfo.put(i, reducer_info.substring(0,reducer_info.length()-1));
				}
			}
		}

		// Start Reducer phase
		for(int i = 1;i  <  serverPortList.size(); i++)
			dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "DO_REDUCE:"+slaveReducerInfo.get(i));


		// Keep Listening to all the slaves to check if mappers finished their jobs

		try
		{
			int stop_count = 0;
			System.out.println("Server listening at port: "+masterPort);
			while(true)
			{
				for(int i = 1;i  <  serverPortList.size(); i++)
					dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "GET_REDUCER_STATUS:");
				s = serverSock.accept();
				BufferedReader inFromClient = new BufferedReader(new InputStreamReader(s.getInputStream()));
				String line = inFromClient.readLine();

				if(line.startsWith("SENDING_REDUCER_STATUS"))
				{
					stop_count += Integer.parseInt(line.split(":")[1]);
				}
				System.out.println(stop_count + "========= Stop Count");
				if(stop_count == numOfMachines)
				{
					// Reducer phase of all machines is done
					for(int i = 1;i  <  serverPortList.size(); i++)
						dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "KILL_YOURSELF:");
					break;
				}
				Thread.sleep(11000);
			}
			s.close();
			serverSock.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

		System.out.println("Finished all Phases.....Congratulations.....");
	}

	// COMMUNICATE THE MESSAGE ACROSS NETWORK
	public static void dispatchSendMessage(String dns, int port, String message) throws UnknownHostException, IOException{
		Socket clientSocket = new Socket(dns, port);
		DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
		outToServer.writeBytes(message + '\n');
		outToServer.flush();
		outToServer.close();
		clientSocket.close(); 
	}

}