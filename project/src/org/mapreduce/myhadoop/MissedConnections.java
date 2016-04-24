package org.mapreduce.myhadoop;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

public class MissedConnections {
	public static class M{
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if (!(line.startsWith("YEAR"))) {
				line = line.replace(", ", "|");
				line = line.replace("\"", "");
				String[] field = line.split(",");
				if (field.length == 110 ) {
					String year = field[0];
					String month = field[2];
					String dayOfMonth = field[3];
					String airline = field[6];
					String origin = field[11];
					String dest = field[20];
					String scheduled_dep = field[29].trim().equals("") ? "NA" : field[29];
					String actual_dep = field[30].trim().equals("") ? "NA" : field[30];
					String scheduled_arr = field[40].trim().equals("") ? "NA" : field[40];
					String actual_arr = field[41].trim().equals("") ? "NA" : field[41];	
					if (!corruptLine(field)) {
						String row   = year + ":" + month + ":" + dayOfMonth + ":" +  airline+ ":"+ origin + ":" + dest + ":" + scheduled_dep + ":" + scheduled_arr + ":" + actual_arr+":"+actual_dep;
						String key_arline = airline+","+year;
						context.write(new Text(key_arline), new Text(row));
					}
				}
			}
		}
	}
	public static int getMin(String hhmm) {
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
	public static boolean corruptLine(String[] field)
	{
		int CRSArrTime = getMin(field[40]);
		int CRSDepTime = getMin(field[29]);
		int CRSElapsedTime = Integer.parseInt(field[50]);
		if (CRSArrTime == 0 || CRSDepTime == 0) {
			return true;
		}
		int timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime;
		if (timeZone % 60 != 0) {
			return true;
		}
		int AirportID = Integer.parseInt(field[20]), AirportSeqID = Integer
				.parseInt(field[21]),
				CityMarketID = Integer
				.parseInt(field[22]),
				StateFips = Integer.parseInt(field[26]
						.replace("\"", "")),
				Wac = Integer.parseInt(field[28]);
		if (AirportID <= 0 || AirportSeqID <= 0 || CityMarketID <= 0
				|| StateFips <= 0 || Wac <= 0) {
			return true;
		}
		String Origin = field[14], Destination = field[23], OriginCityName = field[15], DestinationCityName = field[24],
				OriginState = field[16], DestinationState = field[25], OriginStateName = field[18],
				DestinationStateName = field[27];
		if (Origin.equals("") || Destination.equals("")
				|| OriginCityName.equals("") || DestinationCityName.equals("")
				|| OriginState.equals("") || DestinationState.equals("")
				|| OriginStateName.equals("")
				|| DestinationStateName.equals("")) {
			return true;
		}
		int Cancelled = Integer.parseInt(field[47]);
		if (Cancelled != 1) {
			int ArrTime = getMin(field[41].replace("\"", "")), DepTime = getMin(field[30]
					.replace("\"", "")),
					ActualElapsedTime = Integer
					.parseInt(field[51].replace("\"", ""));
			if (((ArrTime - DepTime - ActualElapsedTime - timeZone) / 60) % 24 != 0) {
				return true;
			}
		
			double ArrDelay = Double.parseDouble(field[42]), ArrDelayMinutes = Double
					.parseDouble(field[43]),
					ArrDelay15 = Double
					.parseDouble(field[44]);
			if (ArrDelay > 0 && ArrDelay != ArrDelayMinutes) {
				return true;
			}
			if (ArrDelay < 0 && ArrDelayMinutes != 0) {
				return true;
			}
			if (ArrDelayMinutes >= 15 && ArrDelay15 != 1) {
				return true;
			}
		}
		return false;
	}
	public  static class R  {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// HashMaps to store list of origins and destinations
			Map<String,HashMap<Integer,LinkedList<String>>>  originMap = new HashMap<String,HashMap<Integer,LinkedList<String>>>();
			Map<String,HashMap<Integer,LinkedList<String>>>  destMap = new HashMap<String,HashMap<Integer,LinkedList<String>>>();
			for (Text val : values) {
				String field[] = val.toString().split(":");
				int year = Integer.parseInt(field[0]);
				int month = Integer.parseInt(field[1]);
				int dayOfMonth = Integer.parseInt(field[2]);
				String origin = field[4];
				String dest = field[5];
				// Hashmap that stores a set of flights for a day of the year(1-365)
				HashMap<Integer,LinkedList<String>> row = new HashMap<Integer,LinkedList<String>>();
				Calendar c = Calendar.getInstance();
				c.set(year, month-1, dayOfMonth);
				int dayOfYear = c.get(Calendar.DAY_OF_YEAR);
				String scheduled_dep = field[6];
				String scheduled_arr = field[7];
				String actual_arr = field[8];
				String actual_dep = field[9];
				// Checking for correct values of dates and start filling HashMaps
				if(!actual_arr.equals("NA") && !scheduled_arr.equals("NA"))
				{
					if(originMap.containsKey(dest))
					{
						row = originMap.get(dest);
						if(row.containsKey(dayOfYear))
						{
							LinkedList<String> eachDay = row.get(dayOfYear);
							eachDay.add(val.toString());
							row.put(dayOfYear,eachDay);
						}
						else
						{
							LinkedList<String> eachDay = new LinkedList<String>();
							eachDay.add(val.toString());
							row.put(dayOfYear,eachDay);
						}
					} else {
						LinkedList<String> eachDay = new LinkedList<String>();
						eachDay.add(val.toString());
						row.put(dayOfYear,eachDay);
					}
					originMap.put(dest, row);
				}
				row = new HashMap<Integer,LinkedList<String>>();
				// Checking for correct values of dates
				if(!scheduled_dep.equals("NA") && !actual_dep.equals("NA"))
				{
					if(destMap.containsKey(origin))
					{
						row = destMap.get(origin);
						if(row.containsKey(dayOfYear))
						{
							LinkedList<String> eachDay = row.get(dayOfYear);
							eachDay.add(val.toString());
							row.put(dayOfYear,eachDay);
						}
						else
						{
							LinkedList<String> eachDay = new LinkedList<String>();
							eachDay.add(val.toString());
							row.put(dayOfYear,eachDay);
						}
					} else {
						LinkedList<String> eachDay = new LinkedList<String>();
						eachDay.add(val.toString());
						row.put(dayOfYear,eachDay);
					}
					destMap.put(origin, row);
				}
			}
			// Hashtables have been populated
			int missed = 0;
			int totalConn = 0;
			for(String airport : originMap.keySet())
			{
				if(destMap.containsKey(airport))
				{
					HashMap<Integer,LinkedList<String>> originsForDate = originMap.get(airport);
					HashMap<Integer,LinkedList<String>> destsForDate = destMap.get(airport);
					for(int dayOfYear : originsForDate.keySet())
					{
						if(destsForDate.containsKey(dayOfYear))
						{
							LinkedList<String> combinedList = new LinkedList<String>();
							combinedList.addAll(destsForDate.get(dayOfYear));
							if(destsForDate.containsKey(dayOfYear+1))
								combinedList.addAll(destsForDate.get(dayOfYear+1)); 
							// Given a pair of linkedlists, get connections and missed connections
							int res[] = getMissedConnections(originsForDate.get(dayOfYear),combinedList);
							missed += res[0];
							totalConn += res[1];
						}
					}
				}
			}
			context.write(key, new Text(missed+" "+ (float)(missed*100.0/totalConn)));
			System.out.println("Key:"+key + " Missed:"+missed+ " Total Connections:"+totalConn + " "  + (float)(missed*100.0/totalConn));
		}
	}
	public static int[] getMissedConnections(LinkedList<String> orig, LinkedList<String> dest)
	{
		int result[] = new int[2];
		int missedConn = 0;
		int connections = 0;
		//Loop through each entry in dest for every value in orig
		for(String or : orig)
		{
			String field1[] = or.split(":");
			int scheduled_arr = getMin(field1[7]);
			int act_arr = getMin(field1[8]);	
			int originDay = Integer.parseInt(field1[2]); 
			for(String d : dest )
			{
				String field2[] = d.split(":");
				int destDay = Integer.parseInt(field2[2]); 
				int scheduled_dep= getMin(field2[6]);
				int actual_dep= getMin(field2[9]);
				// Handling case where dates overalap
				if(destDay == originDay + 1)
				{
					int diff = scheduled_dep +  (24*60 - scheduled_arr);
					if(diff >=30 && diff <=360 )
					{
						//Connection found
						connections++;
						if((actual_dep - (1440-act_arr)) < 30)
						{
							//Missed Connection found
							missedConn ++;
						}
					}
				}
				else {
					int diff = scheduled_dep - scheduled_arr;
					if(diff >= 30 && diff <= 360 )
					{
						//Connection found
						connections++;
						if((actual_dep - act_arr) < 30)
						{
							//Missed Connection found
							missedConn ++;
						}
					}
				}
			}
		}
		result[0] = missedConn;
		result[1] = connections;
		return result;
	}
}
