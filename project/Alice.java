import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Alice {

	public static class M extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)

				throws IOException, InterruptedException {

			String line = value.toString();

			String[] words = line.split(" ");
			for(String word : words){

				//System.out.println("Word="+word);
				context.write(new Text(word.toLowerCase()),new Text(word.toLowerCase()));
			}
		}
	}


	public static class R extends Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterable<Text> values, Context context)

				throws IOException, InterruptedException {


			int count = 0;

			for (Text val : values)
			{
				count ++;
			}

			context.write(key,new Text(count+""));
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "LinearRegression");
		job.setJarByClass(Alice.class);
		job.setMapperClass(M.class);
		job.setReducerClass(R.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
