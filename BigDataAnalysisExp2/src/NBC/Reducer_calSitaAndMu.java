package NBC;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer_calSitaAndMu extends Reducer<Text, DoubleWritable, Text, Text>{
 
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		String StringKey = key.toString();
		Iterator<DoubleWritable> iter = values.iterator();
		if(StringKey.endsWith("T") || StringKey.endsWith("F")) {
			context.write(key, new Text(iter.next().toString()));
		}else {
			double value = 0;
			double sum = 0;
			while(iter.hasNext()) {
				value = iter.next().get();
				sum += value;
			}
			String classification = StringKey.split("\t")[0].split(",")[1];
			String mu = StringKey.split("\t")[0].split("_")[1];
			double classSums = 0;
			if(classification.equals("F")) {
				classSums = context.getConfiguration().getDouble("F", 0);
			}else {
				classSums = context.getConfiguration().getDouble("T",0);
			}
			String MuAndSita = mu +","+String.valueOf(sum/classSums);
			context.write(new Text(StringKey.split("_")[0]), new Text(MuAndSita));
		}
	}
}
