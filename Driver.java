import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class Driver extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		ToolRunner.run(new Driver(), args);
	}
	
	public int run(String[] args) throws Exception{
		// Create configuration
		Configuration conf = new Configuration(true);
		conf.addResource(new Path("C:\\hadoop\\etc\\hadoop\\core-site.xml"));
	    conf.addResource(new Path("C:\\hadoop\\etc\\hadoop\\hdfs-site.xml"));
		//Creating Distributed Cache
		DistributedCache.addCacheFile(new URI("/afnin/AFINN.txt"),conf);

		// Create key & Submitting key
		Job key = new Job(conf, "SentimentAnalysis");
		key.setJarByClass(Driver.class);

		// Setup MapReduce
		key.setMapperClass(Mapper1.class);
		key.setReducerClass(Reducer1.class);

		// Specify key / value
		key.setOutputKeyClass(Text.class);
		key.setOutputValueClass(IntWritable.class);

		// Input
		FileInputFormat.addInputPath(key, new Path(args[0]));
		key.setInputFormatClass(TextInputFormat.class);

		// Output
		FileOutputFormat.setOutputPath(key, new Path(args[1]));
		key.setOutputFormatClass(TextOutputFormat.class);

		//Second JOB
		boolean complete = key.waitForCompletion(true);

		Configuration conf2 = new Configuration();
		conf2.addResource(new Path(""C:\\hadoop\\etc\\hadoop\\core-site.xml"));
	    conf2.addResource(new Path("C:\\hadoop\\etc\\hadoop\\hdfs-site.xml"));
		Job key2 = Job.getInstance(conf2, "chaining");

		//For Mapper2 and Reducer2, second key
		if (complete) {
				key2.setJarByClass(Driver.class);
				
				key2.setMapperClass(Mapper2.class);
				key2.setMapOutputKeyClass(Text.class);
				key2.setMapOutputValueClass(IntWritable.class);
				
				key2.setReducerClass(Reducer2.class);
				key2.setOutputKeyClass(Text.class);
				key2.setOutputValueClass(IntWritable.class);
				

				FileInputFormat.addInputPath(key2, new Path(args[1]));
				FileOutputFormat.setOutputPath(key2, new Path(args[2]));

				System.exit(key2.waitForCompletion(true) ? 0 : 1);
				}
			return 0;
	}
}