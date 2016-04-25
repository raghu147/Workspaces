// Authors Karishma Raj, Nephi Calvin, Nikhil Sudireddy, Raghuveer Ramesh
package org.mapreduce.myhadoop;
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

	private static Integer PSUEDO_MAPPERS_COMPLETED = 0;
	private static Integer PSUEDO_REDUCERS_COMPLETED = 0;
	public static String className = "";
	public static String inputFolderPath = "";
	public static String intermediateFolderPath = "";
	public static String outputFolderPath = "";
	public static String inputType = "";

	/*
	 * Synchronized methods to increment static variables
	 * These variables are used to determine the number of
	 * reducers, mappers finished in a particular machine
	 */
	public static synchronized void reducerInc(){
		PSUEDO_REDUCERS_COMPLETED++;
	}

	public static synchronized int reducerGet(){
		return PSUEDO_REDUCERS_COMPLETED;
	}

	public static synchronized void mapperInc(){
		PSUEDO_MAPPERS_COMPLETED++;
	}

	public static synchronized int mapperGet(){
		return PSUEDO_MAPPERS_COMPLETED;
	}

	/*
	 * Init method to create the folders
	 */

	public static void init(){

		File dir = new File(intermediateFolderPath);

		if(dir.exists() && dir.isDirectory()){
			File [] fs = dir.listFiles();
			for(File f  : fs)
				f.delete();

			dir.delete();

		} 
		dir.mkdir();
		dir = new File(outputFolderPath);


		if(dir.exists() && dir.isDirectory()){
			File [] fs = dir.listFiles();
			for(File f  : fs)
				f.delete();

			dir.delete();
			dir.mkdir();
		} 
		dir.mkdir();

	}


	/* <MainClass> <Input> <Intermediate> <Output> */
	public static void main(String arg[]){

		className = arg[0];
		inputFolderPath = arg[1];
		intermediateFolderPath = arg[2];
		outputFolderPath = arg[3];

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

				// Check if file format is text/gzip
				if(inputType.equals("") && file.getName().endsWith("txt"))
					inputType = "txt";
				else if (inputType.equals("") && file.getName().endsWith("gz"))
					inputType = "zip";


				fileList.add(file.getAbsolutePath());
			}
		}

		//----------------------------------------------------------------------------------------
		//										MAPPER PHASE
		//----------------------------------------------------------------------------------------

		/*
		 * Calculate number of mappers
		 * And also Files to be read by each mapper
		 */
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

		/*
		 * Start all the mapper threads
		 */
		for(int i = 0; i < actualMapperCount; i++){

			PseudoMapperThread pm = new PseudoMapperThread(i,map.get(i));
			executor.execute(pm);
		}

		/*
		 * Check for completion of all the mappers
		 */
		while(true){

			synchronized (PSUEDO_MAPPERS_COMPLETED) {
				if(PSUEDO_MAPPERS_COMPLETED.intValue() == numberOfMappers){
					executor.shutdown();
					break;
				}
			}
		}

		System.out.println("Finished Mapper Phase");

		//----------------------------------------------------------------------------------------
		//										INTERMEDIATE PHASE
		//----------------------------------------------------------------------------------------


		/*
		 * Calculate the number of keys from the intermediate folder
		 */
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

		//----------------------------------------------------------------------------------------
		//										REDUCER PHASE
		//----------------------------------------------------------------------------------------

		executor = Executors.newFixedThreadPool(NUM_THREADS);

		int r = 0;
		/*
		 * Start n reducer threads
		 * n - number of keys
		 */
		for(String key : keySet.keySet()){

			PseudoRedcuerThread t = new PseudoRedcuerThread(new Text(key), keySet.get(key), Pseudo.intermediateFolderPath,r);
			r++;
			executor.execute(t);

		}
		System.out.println("Number of Reducers="+numberOfReducers);

		while(true){

			if(Pseudo.reducerGet() == numberOfReducers){
				break;
			}

		}

		executor.shutdown();

		System.out.println("Finished Reducer Phase");


	}

}


/* 
 * <MainClass> <input-folder> <intermediate-folder> <out-put-folder> 
 * Reducer Thread
 * */
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
		ctx = new Context(reducerNumber, Context.REDUCER_TYPE, reducerKey);

	}


	@Override
	public void run() {

		/*
		 * Use Java Reflections to get reduce method of respective class
		 */
		Class<?> c = null;
		Method method = null;
		try 
		{
			c = Class.forName(Pseudo.className+"$R");
			// Get reduce method
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

		// Read from the list of files that the reducer is supposed to read
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

		// Create a Iterable of all the values
		Iterable<Text> iter = Arrays.asList(arr);


		try {
			// invoke the reducer method
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
			// Finally write the file into local directory.
			ctx.writeToLocalDisk(reducerNumber);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

	}

}

/*
 * Mapper Thread
 */
class PseudoMapperThread implements Runnable{

	List<String> filesToRead;
	public static int MAPPER_STATUS = 0;
	int mapperNumber;
	Context ctx ;

	public PseudoMapperThread(int mapperNumber,List<String> filesToRead) {
		this.filesToRead = filesToRead;
		this.mapperNumber = mapperNumber;
		ctx = new Context(mapperNumber, Context.MAPPER_TYPE, new Text(""));

	}


	@Override
	public void run() {

		/*
		 * Use Java Reflections to get map method of respective Class
		 */
		Class<?> c = null;
		Method method = null;
		try 
		{
			c = Class.forName(Pseudo.className+"$M");
			// Get map method
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



		// Iterate over all the files in input folder
		for(String file : filesToRead){

			BufferedReader reader = null;

			try {

				// If input file type is gzip
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
						// invoke map method
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
			try {
				// Finally write to local directory
				ctx.writeToLocalDisk(mapperNumber);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
