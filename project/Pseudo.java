import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.zip.GZIPInputStream;

public class Pseudo{

	public static Integer PSUEDO_MAPPERS_COMPLETED = 0;
	public static Integer PSUEDO_REDUCERS_COMPLETED = 0;
	public static String className = "ClusterAnalysis";
	//public static String inputFolderPath = "input";
	public static String inputFolderPath = "/home/raghu/Desktop/mr/assignments/small";
	public static String intermediateFolderPath = "mapper-temp";
	public static String outputFolderPath = "reducer-temp";
	public static String inputType = "";

	public static void init(){

		File dir = new File(intermediateFolderPath);


		if(dir.exists() && dir.isDirectory()){
			File [] fs = dir.listFiles();
			for(File f  : fs)
				f.delete();

			dir.delete();
			dir.mkdir();
		} 
		
		dir = new File(outputFolderPath);


		if(dir.exists() && dir.isDirectory()){
			File [] fs = dir.listFiles();
			for(File f  : fs)
				f.delete();

			dir.delete();
			dir.mkdir();
		} 
		
		

	}

	public static void pseudoMode(String arg[]){


		init();

		Double BLOCK = 24.0;
		int NUM_THREADS = 2;

		int numOfFiles = 0;
		File inputDir = new java.io.File(inputFolderPath);
		Double inputSize = 0.0;

		List<String> fileList = new ArrayList<String>();
		ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
		for (File file : inputDir.listFiles()) {
			if (file.isFile()){
				inputSize += file.length();	
				numOfFiles ++;
				
				if(inputType.equals("") && file.getName().endsWith("txt"))
					inputType = "txt";
				else if (inputType.equals("") && file.getName().endsWith("gz"))
				inputType = "zip";
					
				
				fileList.add(file.getAbsolutePath());
			}
		}

		inputSize = inputSize/1024/1024;

		int numberOfMappers  =  (int)Math.max(inputSize/ BLOCK,1);

		int filesPerMapper =   (int)Math.floor(numOfFiles / numberOfMappers);

		Map<Integer,List<String>> map = new HashMap<Integer,List<String>>();
		System.out.println("numberOfMappers="+numberOfMappers + " filesPerMapper="+filesPerMapper);
		int k = 0;
		int mCount = 0;
		List<String> fl = new ArrayList<String>();
		for(int i = 0; i < fileList.size();i++){

			fl.add(fileList.get(i));
			k++;

			if(k == filesPerMapper){
				map.put(mCount,fl);
				mCount ++;
				k = 0;
				fl = new ArrayList<String>();
			}

		}

		if(!fl.isEmpty()){
			List<String> x = map.get(mCount-1);
			x.addAll(fl);
			map.put(mCount-1,x);
		}

		int actualMapperCount = map.size();

		for(int i = 0; i < actualMapperCount; i++){

			PseudoMapperThread pm = new PseudoMapperThread(i,map.get(i) , "");
			executor.execute(pm);
		}

		while(true){

			synchronized (PSUEDO_MAPPERS_COMPLETED) {
				if(PSUEDO_MAPPERS_COMPLETED.intValue() == numberOfMappers){
					executor.shutdown();
					break;
				}
			}
		}

		System.out.println("Finished Mapper Phase");


		// Do Intermediate Phase


		File intermediateDir = new java.io.File(Pseudo.intermediateFolderPath);
		Map<String,List<String>> keySet = new HashMap<String,List<String>>();
		for (File f : intermediateDir.listFiles()) {
			if (f.isFile()){

				String emittedKey = f.getName().split("_")[1];

				List<String> keyFiles = new ArrayList<String>();
				if(keySet.containsKey(emittedKey)){

					keyFiles = keySet.get(emittedKey);
				}

				keyFiles.add(f.getAbsolutePath());
				keySet.put(emittedKey,keyFiles);
			}
		}


		int numberOfReducers = keySet.size();		

		System.out.println("Finished InterMediate Phase");

		executor = Executors.newFixedThreadPool(NUM_THREADS);

		int r = 0;
		for(String key : keySet.keySet()){

			PseudoRedcuerThread t = new PseudoRedcuerThread(new Text(key), keySet.get(key), Pseudo.intermediateFolderPath,r);
			r++;
			executor.execute(t);
			
		}

		while(true){

			synchronized (PSUEDO_REDUCERS_COMPLETED) {
				if(PSUEDO_REDUCERS_COMPLETED.intValue() >= numberOfReducers){
					executor.shutdown();
					break;
				}
			}
		}

		System.out.println("Finished Reducer Phase");


	}

}

class PseudoRedcuerThread implements Runnable{

	List<String> filesToRead;
	public static int MAPPER_STATUS = 0;
	int reducerNumber;
	String interMapPath;
	Context ctx ;
	Text reducerKey;

	public PseudoRedcuerThread(Text reducerKey,List<String> filesToRead,String interMapPath,int reducerNumber) {
		this.filesToRead = filesToRead;
		this.reducerKey = reducerKey;
		this.interMapPath = interMapPath;
		this.reducerNumber = reducerNumber;
		ctx = new Context(reducerNumber, Context.REDUCER_TYPE, reducerKey,interMapPath);

	}


	@Override
	public void run() {

		Class<?> c = null;
		Method method = null;
		try 
		{
			c = Class.forName(Pseudo.className+"$R");
			method = c.getMethod("reduce",new Class[] { Text.class, Iterable.class, Context.class});
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}


		List<Text> lines = new ArrayList<Text>();

		for(String file : filesToRead){

			BufferedReader reader = null;
			try {


				reader = new BufferedReader(new FileReader(file));
				String readline ="";

				while((readline = reader.readLine()) != null)
				{
					lines.add(new Text(readline));
				}	

			}
			catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}


		}

		Text[] arr = new Text[lines.size()];

		for(int i = 0; i < arr.length;i++)
			arr[i] = lines.get(i);

		Iterable<Text> iter = Arrays.asList(arr);


		try {
			method.invoke(c.newInstance(), reducerKey, iter, ctx);
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		}


		try {
			ctx.writeToLocalDisk(reducerNumber);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}

class PseudoMapperThread implements Runnable{

	List<String> filesToRead;
	public static int MAPPER_STATUS = 0;
	int mapperNumber;
	String interMapPath;
	Context ctx ;

	public PseudoMapperThread(int mapperNumber,List<String> filesToRead,String interMapPath) {
		this.filesToRead = filesToRead;
		this.mapperNumber = mapperNumber;
		this.interMapPath = interMapPath;
		ctx = new Context(mapperNumber, Context.MAPPER_TYPE, new Text(""),interMapPath);

	}


	@Override
	public void run() {



		Class<?> c = null;
		Method method = null;
		try 
		{
			c = Class.forName(Pseudo.className+"$M");
			method = c.getMethod("map",new Class[] { LongWritable.class, Text.class, Context.class});
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		}



		for(String file : filesToRead){

			BufferedReader reader = null;

			try {

				if(Pseudo.inputType.equals("zip")){

					InputStream is = new GZIPInputStream(new FileInputStream(file));
					Reader decoder = new InputStreamReader(is);
					reader = new BufferedReader(decoder);
				}
				else{
					reader = new BufferedReader(new FileReader(file));
				}

				String readline ="";
				while((readline = reader.readLine()) != null)
				{
					try {
						method.invoke(c.newInstance(), new LongWritable(""), new Text(readline), ctx);
					} catch (IllegalAccessException e) {
						e.printStackTrace();
					} catch (IllegalArgumentException e) {
						e.printStackTrace();
					} catch (InvocationTargetException e) {
						e.printStackTrace();
					} catch (InstantiationException e) {
						e.printStackTrace();
					}
				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			catch (IOException e) {
				e.printStackTrace();
			}

		}

		try {
			ctx.writeToLocalDisk(mapperNumber);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}