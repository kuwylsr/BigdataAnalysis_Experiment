package LogisticRegression;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_Predict extends Mapper<LongWritable, Text, Text, DoubleWritable>{

	double[] parameters= new double[19];
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String[] s = context.getConfiguration().get("parameters").split(",");
		for(int i = 0 ;i < s.length ;i++) {
			parameters[i] = Double.parseDouble(s[i]);
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] attributions = line.split("\t");
		String classification = attributions[0];
		if (Double.parseDouble(classification) == 1) {
			classification = "F";
		} else {
			classification = "T";
		}
		double[] xi = new double[attributions.length]; //样本的数组的长度为特征数+1
		xi[0] = 1;//样本数组的第一列均为1,方便求解W0
		for(int i = 1; i< xi.length ;i++) { //赋值特征
			xi[i] = Double.valueOf(attributions[i]);
		}
		double rule = 0 ;
		for(int i = 0 ;i < parameters.length;i++) {
			rule += parameters[i] * xi[i];
		}
		
		if(rule <= 0) { //预测为阳
			if(classification.equals("T")) {
				context.write(new Text("Tp"), new DoubleWritable(1));
			}else {
				context.write(new Text("Fp"), new DoubleWritable(1));
			}
		}else { //预测为阴
			if(classification.equals("T")) {
				context.write(new Text("Fn"), new DoubleWritable(1));
			}else {
				context.write(new Text("Tn"), new DoubleWritable(1));
			}
		}		
	}
}
