package LogisticRegression;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_LogisticR extends Reducer<Text, Text, Text, Text>{

	
	double[] parameters = new double[19];
	double[] preParameters= new double[19];
	double lambda;
	double alpha;
	
	@Override
	protected void setup(Reducer<Text, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String[] s = context.getConfiguration().get("parameters").split(",");
		for(int i = 0 ;i < s.length ;i++) {
			preParameters[i] = Double.parseDouble(s[i]);
			//parameters[i] = Double.parseDouble(s[i]) - (alpha * lambda * preParameters[i]);
			parameters[i] = Double.parseDouble(s[i]);
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
	
	@Override
	protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		int flag = 0;
		double precision = context.getConfiguration().getDouble("precision", 0);
		for(int i = 0;i<preParameters.length;i++) {
			if(Math.abs(preParameters[i] - parameters[i])>precision) {
				System.out.println(Math.abs(preParameters[i] - parameters[i]));
				flag = 1;
			}
		}
		if(flag == 0) {
			context.getCounter("tag","tag").setValue(1);
		}
	}
}
