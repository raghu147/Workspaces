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

class Text {

	String val;
	public Text(String lowerCase) {
		
		this.val = lowerCase.toLowerCase();
	}
	
	@Override
	public String toString() {
		return val;
	}
	

}

class LongWritable {

}

class Configuration {

}

class Reducer<T1, T2, T3, T4> {

}

class Mapper<T1, T2, T3, T4> {

}

class Job {

	public static Job getInstance(Configuration conf, String string) {
		return null;
	}

	public void setJarByClass(Class<Alice> class1) {

	}

	public <M> void setMapperClass(Class<M> class1) {

	}

	public <R> void setReducerClass(Class<R> class1) {

	}

	public void setOutputKeyClass(Class<Text> class1) {

	}

	public void setOutputValueClass(Class<Text> class1) {

	}

}



public class Alice {

	public static class M extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context)

				throws IOException, InterruptedException {

			String line = value.toString();

			String[] words = line.split(" ");
			for (String word : words) {

				// System.out.println("Word="+word);
				context.write(new Text(word.toLowerCase()), new Text(word.toLowerCase()));
			}
		}
	}

	public static class R {

		public static void reduce(Text key, List<Text> values, Context context)

				throws IOException, InterruptedException {
			
			
			System.out.println("REFLECTING !!!!");

			int count = 0;

			for (Text val : values) {
				
				System.out.println(val.toString());
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
