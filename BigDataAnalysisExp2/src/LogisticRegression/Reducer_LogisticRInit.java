package LogisticRegression;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_LogisticRInit extends Reducer<Text, Text, Text, Text>{

	double[] parameters = new double[19];
	
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		for(int i = 0 ; i<parameters.length;i++) { //为参数数组赋初值为0
			parameters[i] = 0;
		}
	}
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
		Iterator<Text> iter = values.iterator();
		while(iter.hasNext()) {
			String[] gradients = iter.next().toString().split(",");
			for(int i = 0 ;i<gradients.length;i++) {
				parameters[i] += Double.parseDouble(gradients[i]);
			}
		}
		StringBuilder sb = new StringBuilder();
		int i = 0;
		for(i = 0 ;i<parameters.length-1;i++) {
			sb.append(parameters[i]).append(",");
		}
		sb.append(parameters[i]);
		context.write(null, new Text(sb.toString()));
	}
}
