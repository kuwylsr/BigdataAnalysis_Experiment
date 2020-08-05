package LogisticRegression;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_Predict extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	int Tp = 0;
	int Tn = 0;
	int Fp = 0;
	int Fn = 0;
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		Iterator<DoubleWritable> iter = values.iterator();
		
		while(iter.hasNext()) {
			double temp = iter.next().get();
			if(key.toString().equals("Tp")) {
				Tp += temp;
			}else if(key.toString().equals("Tn")) {
				Tn += temp;
			}else if(key.toString().equals("Fp")) {
				Fp += temp;
			}else {
				Fn += temp;
			}
		}
	}
	
	@Override
	protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println(Tp);
		System.out.println(Tn);
		System.out.println(Fp);
		System.out.println(Fn);
		context.write(new Text("正确率:"), new DoubleWritable((Tp + Tn)/(double)(Tp + Tn + Fp + Fn)));
		context.write(new Text("召回率:"), new DoubleWritable((Tp)/(double)(Tp + Fn)));
		context.write(new Text("精确率:"), new DoubleWritable((Tp)/(double)(Tp + Fp)));
	}
}
