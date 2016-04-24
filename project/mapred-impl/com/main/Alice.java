import java.util.List;

/*import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;*/

public class Alice {

	public static class M extends MyMapper {

		public void map(String key, String value, MyContext context){

			String line = value.toString();

			String[] words = line.split(" ");
			for (String word : words) {

				context.write(word, word);
			}
		}
	}

	public static class R extends MyReducer {

		public void reduce(String key, List<String> values, MyContext context){

			int count = 0;

			for (String val : values) {
				count++;
			}
			context.write(key, count + "");
		}
	}

	public static void main(String[] args) throws Exception {

		Job job = new Job();
		job.setJarByClass(Alice.class);
		job.setMapperClass(M.class);
		job.setReducerClass(R.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(Text.class);
		job.addInputPath(args[0]);
		job.setOutputPath(args[1]);
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
