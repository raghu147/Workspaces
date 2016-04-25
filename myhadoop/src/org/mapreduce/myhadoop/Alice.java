// Authors Karishma Raj, Nephi Calvin, Nikhil Sudireddy, Raghuveer Ramesh
package org.mapreduce.myhadoop;

import java.io.IOException;
import java.util.List;

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


public class Alice {

	public static class M {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split(" ");
			for (String word : words) {
				word = word.replaceAll("[^a-zA-Z^0-9]", "");
				context.write(new Text(word.toLowerCase()), new Text("1"));
			}
		}
	}

	public static class R {

		public static void reduce(Text key, Iterable<Text> values, Context context)

				throws IOException, InterruptedException {
			
			
		

			int count = 0;

			for (Text val : values) {
				
				
				count++;
			}

			context.write(key, new Text(count + ""));
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
