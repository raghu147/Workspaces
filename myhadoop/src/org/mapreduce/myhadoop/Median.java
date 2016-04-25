// Authors Karishma Raj, Nephi Calvin, Nikhil Sudireddy, Raghuveer Ramesh
package org.mapreduce.myhadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 */

public class Median {

	public static class M {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split(" ");
			for (String word : words) {
				word = word.replaceAll("[^a-zA-Z^0-9]", "");
				context.write( new Text("1"),new Text(word.toLowerCase()));
			}
		}
	}

	public static class R {

		public static void reduce(Text key, Iterable<Text> values, Context context)

				throws IOException, InterruptedException {
			
			Map<String,Integer> map = new HashMap<String,Integer>();
			
			for (Text val : values) {
				
				Integer wcount   = 0;
				
				if(map.containsKey(val.toString())){
					wcount = map.get(val.toString());
				}
				
				wcount += 1;
				
				map.put(val.toString(), wcount);
				 
			}
			
			 
			List<Integer> res = new ArrayList<Integer>();
			
			for(String s : map.keySet()){
				res.add(map.get(s));
				
			}
			
			Collections.sort(res);
			
			int size = res.size() ;
			Double median = 0.0;
			
			if(size %2 == 1){
				median = (double)res.get(size/2);
			}
			else{
				median = ((double)res.get(size/2) + (double)res.get(size/2 +1))/2;
			}
			
			System.out.println("Median="+median);

			context.write(new Text("MEDIAN"), new Text(median + ""));
		}
	}

	// public static void main(String[] args) throws Exception {
	//
	// Configuration conf = new Configuration();
	// Job job = Job.getInstance(conf, "LinearRegression");
	// job.setJarByClass(Alice.class);
	// job.setMapperClass(M.class);
	// job.setReducerClass(R.class);
	// job.setOutputKeyClass(Text.class);
	// job.setOutputValueClass(Text.class);
	// FileInputFormat.addInputPath(job, new Path(args[0]));
	// FileOutputFormat.setOutputPath(job, new Path(args[1]));
	// System.exit(job.waitForCompletion(true) ? 0 : 1);
	// }

}


