package exp1_Ultimate;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PCC_Mapper extends Mapper<LongWritable, Text, Text, Text>{

	public static double[] list = new double[6]; 
	public static int lines = 0;
	
	public void map(LongWritable key, Text values,Context context) throws IOException, InterruptedException {
		String line = values.toString();
		String[] attributes = line.split("\\|");
		if(!attributes[6].equals("?")&&!attributes[11].equals("?")) {
			//longitude
			list[0] += Double.parseDouble(attributes[1]);

			//latitude
			list[1] += Double.parseDouble(attributes[2]);
		
			//altitude
			list[2] += Double.parseDouble(attributes[3]);
			
			//temperature
			list[3] += Double.parseDouble(attributes[5].substring(0, attributes[5].length()-1));
			
			//rating
			list[4] += Double.parseDouble(attributes[6]);
			
			//userincome
			list[5] += Double.parseDouble(attributes[11]);
		
			lines++;
			Text t = new Text();
			t.set("1");
			context.write(t, values);
		}
		
		
		
	}
}
