package NBC_lisan;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_calProbability extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		int TrueNums = context.getConfiguration().getInt("T", 0);
		int FalseNums = context.getConfiguration().getInt("F", 0);
		context.write(new Text("T"), new DoubleWritable((TrueNums/(double)(TrueNums + FalseNums))));
		context.write(new Text("F"), new DoubleWritable((FalseNums/(double)(TrueNums + FalseNums))));
		super.setup(context);
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String[] line = value.toString().split("\t");
		String attribution = line[0].split(",")[0];
		String classification = line[0].split(",")[1];
		int attriNums = Integer.parseInt(line[1]);
		int classNums = 0;
		if(Double.parseDouble(classification) == 1) {
			classNums = context.getConfiguration().getInt("F", 0);
			double probability = attriNums/(double)classNums;
			context.write(new Text(attribution), new DoubleWritable(probability));
		}else {
			classNums = context.getConfiguration().getInt("T",0);
			double probability = attriNums/(double)classNums;
			context.write(new Text(attribution), new DoubleWritable(probability));
		}
	}
}
