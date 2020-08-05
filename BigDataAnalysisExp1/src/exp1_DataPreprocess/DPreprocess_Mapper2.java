package exp1_DataPreprocess;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DPreprocess_Mapper2 extends Mapper<LongWritable, Text, Text, Text>{
	
	
	
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String user_income = line.split("\\|")[11];
		String rating = line.split("\\|")[6];
		
		int flag = 0;
		Text identification = new Text();
		Text tLine = new Text();
		tLine.set(line);
		if (user_income.equals("?")) {
			flag = 1;
			identification.set("ui?");
			context.write(identification, tLine);
		}
		if (rating.equals("?")) {
			flag = 1;
			identification.set("r?");
			context.write(identification, tLine);
		}
		
		if(flag == 0) {
			identification.set("normal");
			context.write(identification,tLine);
		}
	}
	
	
	
	
}

