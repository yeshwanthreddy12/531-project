import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> valuesoftext,Context context) throws IOException, InterruptedException {
    	int count = 0;
    	for (IntWritable value : valuesoftext) {
    		count = count + value.get();
		}
    	context.write(new Text(key),new IntWritable(count));	
    }
}