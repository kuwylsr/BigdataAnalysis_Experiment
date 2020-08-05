package NBC_lisan;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_calProbability extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
 
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		Iterator<DoubleWritable> iter = values.iterator();
		while(iter.hasNext()) {
			DoubleWritable dw = iter.next();
			context.write(key, dw);
		}
	}
}
