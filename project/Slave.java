import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class Slave {
	static HashMap<Integer, String> ser_port_hash = new HashMap<Integer, String>();

	public static void main(String[] args) {
		int port = Integer.parseInt(args[0]);
		String inputBucket;
		String outputBucket;
		ServerSocket serverSock = null;
		Socket s = null;	
		try
		{
			serverSock = new ServerSocket(port);
			System.out.println("Server listening at port: "+port);
			while(true)
			{
				s = serverSock.accept();
				BufferedReader inFromClient = new BufferedReader(new InputStreamReader(s.getInputStream()));
				String line = inFromClient.readLine();

				if(line.startsWith("BUCKET_INFO")){
					String buckets = line.split(":")[1];
					inputBucket = buckets.split(",")[0];
					outputBucket = buckets.split(",")[1];
					System.out.println("Bucket info is ........" + line);
				}

				if(line.startsWith("SERVER_PORT_DNS_LIST")){
					String s_p_line = line.split(":")[1];
					String server_port_list[] = s_p_line.split(",");
					for(String sp : server_port_list)
					{
						// server_num, port#dns
						ser_port_hash.put(Integer.parseInt(sp.split("=")[0]), sp.split("=")[1]);
					}
					System.out.println("Server port info is ........" + line);
				}

				if(line.startsWith("MAPPER_INFO")){
					System.out.println("Mapper info in slave is ........" + line);
				}

				if(line.startsWith("KILL_YOURSELF")){
					System.out.println("Killing myself.....");
					break;
				}
			}
			s.close();
			serverSock.close();

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}

	}

}
