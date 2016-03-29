import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Server extends Thread{
	
	int port;
	int serverNumber;
	int numberOfFiles;
	
	List<Double> tempList ;
	
	
	public Server(int p){
		this.port = p;
		
		tempList = new ArrayList<Double>();
	}

	@Override
	public void run() {
		
		ServerSocket serverSock = null;
		Socket s = null;
		
		
		try {
			serverSock = new ServerSocket(port);
			System.out.println("Server listening at port:"+port);
			
			while(true)
			{
				s = serverSock.accept();
				BufferedReader inFromClient =new BufferedReader(new InputStreamReader(s.getInputStream()));
				String line = inFromClient.readLine();
				
				if(line.startsWith("FROM_SERVER")){
					System.out.println("Received line:" + line);
					Double t = Double.parseDouble(line.split(":")[1]);
					tempList.add(t);
					
				}
					
				if(line.startsWith("FIN_SENDING_DATA")){
					break;
				}
				
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
