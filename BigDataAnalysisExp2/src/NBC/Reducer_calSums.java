package NBC;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer_calSums extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
	
	//将结果输出到多个文件
	private MultipleOutputs<Text, DoubleWritable> mos;
	//创建对象
	@Override
	protected void setup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text,DoubleWritable>(context);
	}

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		Iterator<DoubleWritable> iter = values.iterator();
		double value = 0;
		double sum = 0;
		while(iter.hasNext()) {
			value = iter.next().get();
			sum += value;
		}
		if(key.toString().equals("T")||key.toString().equals("F")) {
			mos.write("classificationsNum", key, sum);
		}else {
			context.write(key, new DoubleWritable(sum));
		}		
	}
	
	@Override
	protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}
}
