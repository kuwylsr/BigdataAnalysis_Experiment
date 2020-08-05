package NBC;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_calSums extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] attributions = line.split("\t");
		String temp = "";
		Text t = new Text();
		String classification = attributions[0];
		if(Double.parseDouble(classification) == 1) {
			classification = "F";
			t.set(classification);
			context.write(t, new DoubleWritable(1));
		}else {
			classification = "T";
			t.set(classification);
			context.write(t, new DoubleWritable(1));
		}
		for(int i = 1 ;i<attributions.length;i++) {
			t.set("attribute"+String.valueOf(i)+","+classification);
			context.write(t, new DoubleWritable(Double.parseDouble(attributions[i])));
		}
		
	}
}
