// Authors Karishma Raj, Nephi Calvin, Nikhil Sudireddy, Raghuveer Ramesh
package org.mapreduce.myhadoop;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;

public class Master {
	public static ClientConfiguration clientConfig = new ClientConfiguration();
	static AmazonS3 s3Client = null;

	/*
	 * Split the bucket name to take the input from the final sub directory
	 */
	public static String splitBucketInfo(String bucket)
	{
		String truncatedBucketString = bucket.substring(5);


		int firstSlash = truncatedBucketString.indexOf("/");

		String bucketName = "";
		String bucketprefix = "";

		if(firstSlash != -1){

			bucketName = truncatedBucketString.substring(0, firstSlash);
			bucketprefix = truncatedBucketString.substring(firstSlash+1);

			if(!bucketprefix.endsWith("/"))
				bucketprefix += "/";
		}
		else{
			bucketName = truncatedBucketString;
			bucketprefix = "empty";
		}
		return bucketName+"#"+bucketprefix;

	}

	public static void main(String arg[]) throws UnknownHostException, IOException, InterruptedException
	{
		clientConfig.setConnectionTimeout(50*10000);
		s3Client = new AmazonS3Client(clientConfig);
		// Arguments: ClassName totalMachines input intermediate output masterport port1 port2 ....
		String classToExecute = arg[0];
		int numOfMachines = Integer.parseInt(arg[1]);
		// Parsing the bucket names(Get the bucket name, its prefix)
		String inputBucketName = splitBucketInfo(arg[2]);
		String inputBucket = inputBucketName.split("#")[0];
		String inputPrefix = inputBucketName.split("#")[1].equals("empty") ? "": inputBucketName.split("#")[1];
		String intermediateBucketName = splitBucketInfo(arg[3]);
		String interBucket = intermediateBucketName.split("#")[0];
		String interPrefix = intermediateBucketName.split("#")[1].equals("empty") ? "": intermediateBucketName.split("#")[1];
		String outputBucketName = splitBucketInfo(arg[4]);
		String outputBucket = outputBucketName.split("#")[0];
		String outputPrefix = outputBucketName.split("#")[1].equals("empty") ? "": outputBucketName.split("#")[1];
		int masterPort = Integer.parseInt(arg[5]);

		String dns_file_path = "publicDNS.txt";
		List<Integer> serverPortList = new ArrayList<Integer>();
		List<String> partFileList = new ArrayList<String>();
		String[] serverDNSList = null;
		String server_port_dns="";

		double totalSize = 0.0;
		int BLOCKSIZE = 128;

		int totalNumOfMappers = 0;
		int factor = 0;
		int diff = 0;
		int file_count = 0;
		int startFileIndex = 0;
		int endFileIndex = 0;
		int file_factor = 0;
		int mappersToRead = 0;
		HashMap<Integer, String> slaveInfo = new HashMap<Integer, String>();

		// Read the DNS file, and store it in a string(contains DNS of all machines)
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

		// Copy a combination of server number, server port and server DNS into a string
		// Example: 0=localhost#8890,1=localhost#8891
		for(int i = 0; i <= numOfMachines; i++)
		{
			server_port_dns = server_port_dns + i + "=" + serverPortList.get(i) + "#"+serverDNSList[i] + ",";
		}
		server_port_dns = server_port_dns.substring(0, server_port_dns.length() - 1);


		//----------------------------------------------------------------------------------------
		//										MAPPER PHASE
		//----------------------------------------------------------------------------------------


		/*
		 *  Read S3 bucket to calculate size and number of files
		 */

		ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
				.withBucketName(inputBucket)
				.withPrefix(inputPrefix);
		ObjectListing objectListing;
		do {
			objectListing = s3Client.listObjects(listObjectsRequest);
			for (S3ObjectSummary objectSummary : 
				objectListing.getObjectSummaries()) {
				String key = objectSummary.getKey();
				if(key.equals(inputPrefix))
					continue;
				totalSize += objectSummary.getSize();
				file_count++;
			}
			totalSize /= 1024*1024;
			listObjectsRequest.setMarker(objectListing.getNextMarker());
		} while (objectListing.isTruncated());

		// Calculate number of mappers per machine
		if(totalSize < BLOCKSIZE)
			totalNumOfMappers = 1;
		else
			totalNumOfMappers = (int)totalSize / BLOCKSIZE;

		file_factor = file_count / totalNumOfMappers;
		if(file_factor==0)
			totalNumOfMappers=file_count;

		factor = totalNumOfMappers / numOfMachines;
		diff = totalNumOfMappers - (factor * numOfMachines);

		/*
		 *  Store mappers per machine info in a Hashmap
		 *  Also store the start and end index of the files,
		 *  that the mappers are expected to read.
		 */
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
				// This is for the last machine
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


		// Broadcast server_port_dns info to all slaves
		for(int i = 1;i  <  serverPortList.size(); i++)
			dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "SERVER_PORT_DNS_LIST:"+server_port_dns);

		// Broadcast slave info(Mapper Info) to all slaves
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
				System.out.println(stop_count + " Machines done");
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
				.withBucketName(interBucket)
				.withPrefix(interPrefix);
		ObjectListing objectListingIntermediate;
		int m_count;
		do {
			m_count = 0;
			do {
				objectListingIntermediate = s3Client.listObjects(listObjectsRequestIntermediate);
				for (S3ObjectSummary objectSummary : 
					objectListingIntermediate.getObjectSummaries()) {
					String key=objectSummary.getKey();
					if(key.equals(interPrefix))
						continue;
					String key_value = key.split("_")[1];
					m_count++;
					if(!mrKeys.contains(key_value))
						mrKeys.add(key_value);
				}
				listObjectsRequestIntermediate.setMarker(objectListingIntermediate.getNextMarker());
				System.out.println("MappersToRead, Keys read: " + mappersToRead + "," + m_count);
			} while (objectListingIntermediate.isTruncated());
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

		/*
		 * Calculate the number of reducers per machine and
		 * the Key Files they have to read.
		 */
		for(int i = 1; i<= numOfMachines;i++)
		{
			if(reducer_number < allKeys)
			{
				if(i == numOfMachines)
				{
					// This is for last machine
					int reducers = (reducersPerMachine + reducerDiff);
					String reducer_info = "";
					for(int j = 1; j<=reducers; j++)
					{
						reducer_info += mrKeys.get(reducer_number) + "@" + (reducer_number + 1) + "#";
						reducer_number ++;
					}
					slaveReducerInfo.put(i, reducer_info.substring(0,reducer_info.length()-1));
				}
				else
				{
					String reducer_info = "";
					for(int j=1; j<=reducersPerMachine; j++)
					{
						reducer_info += mrKeys.get(reducer_number) + "@" + (reducer_number + 1) + "#";
						reducer_number ++;
					}
					if(!reducer_info.isEmpty())
						slaveReducerInfo.put(i, reducer_info.substring(0,reducer_info.length()-1));
				}
			}
		}

		/*
		 *  Start Reducer phase
		 *  Send the reducer information to all the machines
		 */
		for(int i = 1;i  <  serverPortList.size(); i++)
		{
			if(!(slaveReducerInfo.get(i) == null))
				dispatchSendMessage(serverDNSList[i], serverPortList.get(i), "DO_REDUCE:"+slaveReducerInfo.get(i)+":"+classToExecute);
		}


		// Keep Listening to all the slaves to check if reducers finished their jobs

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
					stop_count += Integer.parseInt(line.split(":")[1]);

				System.out.println(stop_count + " Machines done");
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

		System.out.println("Finished Reducer Phase....");
		System.out.println("Combining all part files.....");
		// COMBINING ALL PART FILES

		ListObjectsRequest listObjectsRequestPart = new ListObjectsRequest()
				.withBucketName(outputBucket)
				.withPrefix(outputPrefix);
		ObjectListing objectListingPart;
		int part_count;
		do {
			part_count = 0;
			do {
				objectListingPart = s3Client.listObjects(listObjectsRequestPart);
				for (S3ObjectSummary objectSummary : 
					objectListingPart.getObjectSummaries()) {
					String key=objectSummary.getKey();
					if(key.equals(outputPrefix))
						continue;
					if(!partFileList.contains(key))
						partFileList.add(key);
				}
				part_count += objectListingPart.getObjectSummaries().size();
				listObjectsRequestPart.setMarker(objectListingPart.getNextMarker());
			} while (objectListingPart.isTruncated());
		}while(allKeys!=part_count);

		System.out.println("Extracted all " + part_count + " Keys.");

		/* 
		 * Read from each key part file and create a single part file 
		 */ 

		TransferManager partTX = new TransferManager( new ProfileCredentialsProvider());

		// Create a combined part file in local
		File combinedPartFile = null;

		combinedPartFile = new File("reducer-temp/part-r-00000");

		if (!combinedPartFile.exists()) {
			combinedPartFile.createNewFile();
		}

		FileWriter fw = new FileWriter(combinedPartFile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);

		BufferedReader reader = null;
		for(int i=0;i<allKeys;i++){
			S3Object s3object = s3Client.getObject(new GetObjectRequest(
					outputBucket, partFileList.get(i)));

			// Get a range of bytes from an object.
			GetObjectRequest rangeObjectRequest = new GetObjectRequest(
					outputBucket, partFileList.get(i));
			rangeObjectRequest.setRange(0, 10);

			InputStreamReader decoder = null;
			decoder = new InputStreamReader(s3object.getObjectContent());
			reader = new BufferedReader(decoder);
			String readline ="";
			try {
				while((readline = reader.readLine()) != null)
				{
					bw.write(readline+"\n");
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			s3object.close();
		}
		bw.close();
		fw.close();

		// Upload the final part File into s3 bucket
		partTX.upload(outputBucket, combinedPartFile.getName()+".txt", combinedPartFile).isDone();
		System.out.println("Finished all Phases.....Congratulations.....");
		Thread.sleep(2000);
		System.exit(0);
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
