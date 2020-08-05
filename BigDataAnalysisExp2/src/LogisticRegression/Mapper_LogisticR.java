package LogisticRegression;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_LogisticR extends Mapper<LongWritable, Text, Text, Text>{

	double alpha;
	double[] preParameters = new double[19];
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		alpha = context.getConfiguration().getDouble("alpha", 0);
		String[] s = context.getConfiguration().get("parameters").split(",");
		for(int i = 0 ;i < s.length ;i++) {
			preParameters[i] = Double.parseDouble(s[i]);
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] attributions = line.split("\t");
		String classification = attributions[0];
		double[] xi = new double[attributions.length]; //样本的数组的长度为特征数+1
		xi[0] = 1;//样本数组的第一列均为1,方便求解W0
		for(int i = 1; i< xi.length ;i++) { //赋值特征
			xi[i] = Double.valueOf(attributions[i]);
		}
		double[] gradients = refreshGradients(preParameters, xi, alpha, Double.parseDouble(classification)); //迭代更新参数数组
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for(i = 0 ;i<gradients.length-1;i++) {
			sb.append(gradients[i]).append(",");
		}
		sb.append(gradients[i]);
		context.write(new Text("gradients"), new Text(sb.toString()));
	}
	
	
	public static double[] refreshGradients(double[] parameters,double[] xi,double alpha,double y) {
		double wx = 0;
		double[] gradients = new double[19];
		for(int i = 0 ;i < xi.length ;i ++) {
			wx += parameters[i] * xi[i];
		}
		double sigmoid = 1/(1+Math.exp(-wx));
		for(int i = 0 ;i<parameters.length ;i++) {
			gradients[i] = alpha * xi[i] * (y - sigmoid);
		}
		return gradients;
	}
	
}
