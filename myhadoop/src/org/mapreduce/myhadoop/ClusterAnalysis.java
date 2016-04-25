// Authors Karishma Raj, Nephi Calvin, Nikhil Sudireddy, Raghuveer Ramesh
package org.mapreduce.myhadoop;

import java.io.IOException;



import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;



//Class containing Mapper and Reducer classes
public class ClusterAnalysis  {
	//Mapper Class
	public static class M { 
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			String row=value.toString();//Get rows of data
			if(!(row.startsWith("YEAR"))){ //Ignore header line
				row=row.replace("\"", "");
				row=row.replace(", ","|"); //Replace , with | if any
				String[] column=row.split(",");//Split rows into columns
				if(column.length==110) //Check for data integrity
				{
					//System.out.println(column);

					if(!checkSanity(column)){ //Check for sanity
						if (column[0].equals("2015")){ //Send only 2015 values to reducer
							String newkey = column[6];
							String newvalue = column[2] + "#" + column[6] + "#" + column[109];
							context.write(new Text(newkey),new Text(newvalue));
						}
					}
				}
			}

		}
	}
	//Returns the minutes
	public static int getMinutes(String hhmm) {
		if (hhmm.isEmpty()) {
			return 0;
		}

		int min = 0, hr = 0;
		if (hhmm.length() < 3) {
			min = Integer.parseInt(hhmm);
		} else if (hhmm.length() == 3) {
			hr = Integer.parseInt(hhmm.substring(0, 1));
			min = Integer.parseInt(hhmm.substring(1, 3));
		} else if (hhmm.length() == 4) {
			hr = Integer.parseInt(hhmm.substring(0, 2));
			min = Integer.parseInt(hhmm.substring(2, 4));
		}

		return hr * 60 + min;
	}
	//Returns false if data is not  corrupt   
	public static boolean checkSanity(String[] column)
	{
		//System.out.println(column[50]);
		try{
		int crsArrTime = getMinutes(column[40].replace("\"", ""));
		int crsDepTime = getMinutes(column[29].replace("\"", ""));
		int crsElapsedTime = Integer.parseInt(column[50].replace("\"", ""));
		if (crsArrTime == 0 || crsDepTime == 0) {
			return true;
		}

		int timeZone = crsArrTime - crsDepTime - crsElapsedTime;
		if (timeZone % 60 != 0) {
			return true;
		}

		int airportID = Integer.parseInt(column[20]); 
		int airportSeqID = Integer.parseInt(column[21]);
		int cityMarketID = Integer.parseInt(column[22]);
		int stateFips = Integer.parseInt(column[26].replace("\"", ""));
		int wac = Integer.parseInt(column[28]);

		if (airportID <= 0 || airportSeqID <= 0 || cityMarketID <= 0
				|| stateFips <= 0 || wac <= 0) {
			return true;
		}

		String origin = column[14];
		String destination = column[23];
		String originCityName = column[15];
		String destinationCityName = column[24];
		String originState = column[16];
		String destinationState = column[25];
		String originStateName = column[18];
		String destinationStateName = column[27];
		if (origin.equals("") || destination.equals("")
				|| originCityName.equals("") || destinationCityName.equals("")
				|| originState.equals("") || destinationState.equals("")
				|| originStateName.equals("")
				|| destinationStateName.equals("")) {
			return true;
		}

		int cancelled = Integer.parseInt(column[47]);
		if (cancelled != 1) {
			int arrTime = getMinutes(column[41].replace("\"", ""));
			int depTime = getMinutes(column[30].replace("\"", ""));
			int actualElapsedTime = Integer.parseInt(column[51].replace("\"", ""));

			if (((arrTime - depTime - actualElapsedTime - timeZone) / 60) % 24 != 0) {
				return true;
			}


			double arrDelay = Double.parseDouble(column[42]);
			double arrDelayMinutes = Double.parseDouble(column[43]);
			double arrDelay15 = Double.parseDouble(column[44]);
			if (arrDelay > 0 && arrDelay != arrDelayMinutes) {
				return true;
			}

			if (arrDelay < 0 && arrDelayMinutes != 0) {
				return true;
			}

			if (arrDelayMinutes >= 15 && arrDelay15 != 1) {
				return true;
			}

		}
		}catch(NumberFormatException e){
			e.printStackTrace();
			return false;
		}
		
		return false;
		
	}
	//Reducer class
	public static class R extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)  
				throws IOException, InterruptedException {
			// Data Structure to display month, airline and cost
			Map<String,HashMap<String,LinkedList<Double>>> allAirlines = new HashMap<String,HashMap<String,LinkedList<Double>>>();
			for (Text val : values)
			{
				String line = val.toString();
				String[] value = line.split("#");
				String month = value[0];
				String airline = value[1];
				String cost = value[2];
				//If the HashMap does not contain airline add it
				if (!allAirlines.containsKey(airline)) {
					HashMap<String,LinkedList<Double>> newLine = new HashMap<String,LinkedList<Double>>();
					LinkedList<Double> newMonth = new LinkedList<Double>();
					newMonth.add(Double.parseDouble(cost));
					newLine.put(month,newMonth);
					allAirlines.put(airline, newLine);
				}
				//If the airline is existing
				else
				{
					HashMap<String,LinkedList<Double>> existingLine  = allAirlines.get(airline);
					//If it already has the month
					if(!existingLine.containsKey(month))
					{
						LinkedList<Double> newMonth = new LinkedList<Double>();
						newMonth = new LinkedList<Double>();
						newMonth.add(Double.parseDouble(cost));
						existingLine.put(month, newMonth);
					} 
					else 
					{
						LinkedList<Double> existingMonth = existingLine.get(month);
						existingMonth.add(Double.parseDouble(cost));
						existingLine.put(month, existingMonth);
						
					}
					allAirlines.put(airline, existingLine);

				}
			}

			//Iterate through keyset and generate results
			for(String k  : allAirlines.keySet())
			{
				HashMap<String,LinkedList<Double>> eachAirline = allAirlines.get(k);
				for(String k2 : eachAirline.keySet())
				{
					LinkedList<Double> eachMonth = eachAirline.get(k2);
					Double count  = 0.0;
					for(Double d : eachMonth)
						count += d;
					Double avg = count/eachMonth.size();
					String contextText = k + "," + k2 + "," + avg; //Comma Seperated Values
					//Display the result
					context.write(key, new Text(contextText));
				}
			}

		}
	}
	
}
