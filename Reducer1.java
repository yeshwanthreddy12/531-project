import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException
	{
		int sum = 0;
		for(IntWritable val : value)
		{
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}