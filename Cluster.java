import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Cluster {

	public static void main(String arg[]) throws UnknownHostException, IOException{

		String inputFile = "data.txt";
		String range;
		String server_port_Numbers;

		// 4 0 11000 12000 13000 14000 
		int totalServers = Integer.parseInt(arg[0]);
		int serverNumber = Integer.parseInt(arg[1]);

		List<Integer> serverPortList = new ArrayList<Integer>();

		for(int i = 2; i <  arg.length;i++){
			serverPortList.add(Integer.parseInt(arg[i]));
		}


		Thread t1 = new Thread(new Server(serverPortList.get(serverNumber),serverNumber));	

		t1.start();


		// Det Range
		List<String> rangeString = new ArrayList<String>();

		

		if(serverNumber == 0){

			// Input file
			// loop
			//rangeString.add("0.0,5.0");
			//rangeString.add("6.0,10.0");
			range = "0#0.0-50.0,1#51.0-100.0";
			server_port_Numbers = "0-19001,1-19101";
			

			BufferedReader bufferedReader = null;
			FileReader fileReader = null;
			
			/*for(int port : serverPortList)
			{
				dispatchSendMessage(port, "SERVER_PORT_LIST:"+server_port_Numbers);
			}*/
			
			for(int port : serverPortList)
			{
				dispatchSendMessage(port, "RANGE:"+range);
			}
			for(int i = 0;i  <  serverPortList.size(); i++)
			dispatchSendMessage(serverPortList.get(i),"FIN_SENDING_DATA:");

			
			/*try {
				fileReader = new FileReader(inputFile);

				bufferedReader = new BufferedReader(fileReader);
				String line;

				while((line = bufferedReader.readLine()) != null) {

					int temperature = Integer.parseInt(line);

					int server = 0;
					for(String r : rangeString){

						double start = Double.parseDouble(r.split(",")[0]);
						double end =  Double.parseDouble(r.split(",")[1]);

						if(temperature > start && temperature < end){

							int sendToPort = serverPortList.get(server);
							dispatchSendMessage(sendToPort,"FROM_SERVER:"+temperature);
							break;
						}

						server ++;
					}

				}
				
				for(int i = 0;i  <  serverPortList.size(); i++)
				dispatchSendMessage(serverPortList.get(i),"FIN_SENDING_DATA:");


			}   
			catch(Exception e){
				
			}

			bufferedReader.close();    
			fileReader.close();*/

		}


	}
	
	public static void dispatchSendMessage(int port,String message) throws UnknownHostException, IOException{
		
		Client obj = new Client(port,message+"\n");
		Thread sendThread = new Thread(obj);
		sendThread.run();
		obj.send();
	}
	
	public static void dispatchSendObject(int port,List<String>  range) throws UnknownHostException, IOException{
		
		Client obj = new Client(port, range);
		Thread sendThread = new Thread(obj);
		sendThread.run();
		obj.sendObject();
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
	public void sendObject()throws UnknownHostException, IOException {

		Socket clientSocket = new Socket("localhost",port);
		ObjectOutputStream outToServer = new ObjectOutputStream(clientSocket.getOutputStream());
		outToServer.writeObject(range);
		clientSocket.close(); 

	}
}
