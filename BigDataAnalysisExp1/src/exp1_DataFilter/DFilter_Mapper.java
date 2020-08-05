package exp1_DataFilter;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DFilter_Mapper extends Mapper<LongWritable, Text, Text, NullWritable>{

	
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String Srating = line.split("\\|")[6];
		double longitude = Double.parseDouble(line.split("\\|")[1]);
		double latitude = Double.parseDouble(line.split("\\|")[2]);
		
		double leftLimit = context.getConfiguration().getDouble("leftValue", 0);
		double rightLimit = context.getConfiguration().getDouble("rightValue", 0);
		if(longitude >= 8.1461259 && longitude <= 11.1993265 && latitude >= 56.5824856 && latitude <= 57.750511) {
			if(Srating.equals("?")) {
				context.write(value, NullWritable.get());
			}else {
				Double rating = Double.parseDouble(Srating);
				if(rating>leftLimit&&rating<rightLimit) {
					context.write(value, NullWritable.get());
				}
			}
		}
	}
}
