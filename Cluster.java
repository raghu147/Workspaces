import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class Cluster {

	public static void main(String arg[]) throws UnknownHostException, IOException{

		String range;
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

		Thread t1 = new Thread(new Server(serverPortList.get(serverNumber),serverNumber, totalServers));	

		t1.start();

		if(serverNumber == 0){

			server_port = "0-18003,1-18004";
			range = "0#0.0-50.0,1#51.0-100.0";
			
			for(int port : serverPortList)
				dispatchSendMessage(port, "SERVER_PORT_LIST:"+server_port);
			
			for(int port : serverPortList)
				dispatchSendMessage(port, "RANGE:"+range);
			
			for(int i = 0;i  <  serverPortList.size(); i++)
				dispatchSendMessage(serverPortList.get(i),"FIN_SENDING_DATA:");

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
