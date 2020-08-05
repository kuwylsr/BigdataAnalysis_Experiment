package exp1_Ultimate;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DUltimate_Mapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	static double leftLimit;
	static double rightLimit;
	static double max;
	static double min;
	protected void setup(Context context) {
		leftLimit = context.getConfiguration().getDouble("leftValue", 0);
		rightLimit = context.getConfiguration().getDouble("rightValue", 0);
		max = context.getConfiguration().getDouble("max", 0);
		min = context.getConfiguration().getDouble("min", 0);
	}
	public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String Srating = line.split("\\|")[6];
		double longitude = Double.parseDouble(line.split("\\|")[1]);
		double latitude = Double.parseDouble(line.split("\\|")[2]);
		
//		double leftLimit = context.getConfiguration().getDouble("leftValue", 0);
//		double rightLimit = context.getConfiguration().getDouble("rightValue", 0);
		
		//奇异值过滤
		if(longitude >= 8.1461259 && longitude <= 11.1993265 && latitude >= 56.5824856 && latitude <= 57.750511) {
			if(Srating.equals("?")) {
				context.write(value, NullWritable.get());
			}else {
				Double rating = Double.parseDouble(Srating);
				if(rating>leftLimit&&rating<rightLimit) {
					//进行数据标准化和归一化
					String review_date = line.split("\\|")[4];
					String user_birthday = line.split("\\|")[8];
					String temperature = line.split("\\|")[5];
					
					String lineTemp1 = null;
					String lineTemp2 = null;
					try {
						lineTemp1 = standardizingDate(line, review_date);
						lineTemp2 = standardizingDate(lineTemp1, user_birthday);
						
					} catch (ParseException e) {
						e.printStackTrace();
					}
					String lineTemp3 = standardizingTem(lineTemp2, temperature);
					String lineTemp4 = normalizingRating(context, lineTemp3, Srating);
					
					Text text = new Text();
					text.set(lineTemp4);
					context.write(text, NullWritable.get());
				}
				
			}
		}
	}
	
	public String standardizingDate(String line , String date) throws ParseException {
		String[] patterns = new String[3];
		patterns[0] = "[\\d]+\\-[\\d]+\\-[\\d]+";
		patterns[1] = "[\\d]+\\/[\\d]+\\/[\\d]+";
		patterns[2] = "[a-zA-Z]+\\s[\\d]+,[\\d]+";
		
		String[] Spattern = new String[3];
		Spattern[0] = "yyyy-MM-dd";
		Spattern[1] = "yyyy/MM/dd";
		Spattern[2] = "MMMM dd,yyyy";
		
		String lineTemp = "";
		for(int i = 0;i<3;i++) {
			Pattern p = Pattern.compile(patterns[i]);
			Matcher m = p.matcher(date);
			if(m.matches()) {
				DateFormat dateFormat = new SimpleDateFormat(Spattern[i],Locale.US);
				Date dateTemp = dateFormat.parse(date);
				lineTemp = line.replaceAll(patterns[i], dateTemp.toLocaleString().split(" ")[0]);
				break;
			}
		}
		return lineTemp;
	}
	
	public String standardizingTem(String line,String Stemperature) {
		if(Stemperature.contains("℃")) {
			double temperature = Double.parseDouble(Stemperature.replaceAll("℃", ""));
			double tTemp = temperature*1.8+32;
			String lineTemp = line.replaceAll("\\|[\\.,\\-,0-9]+℃\\|", "|"+String.format("%.1f", tTemp)+"℉"+"|");
			return lineTemp;
		}else {
			return line;
		}
		
	}
	
	public String normalizingRating(Context context , String line, String Srating) {
		if(Srating.equals("?")) return line;
		
		double rating = Double.parseDouble(Srating);
//		double max = context.getConfiguration().getDouble("max", 0);
//		double min = context.getConfiguration().getDouble("min", 0);
		double normalRating = (rating - min)/(max - min);
		
		String pattern = "\\|[\\d]+.[\\d]{2}\\|";
		String lineTemp = line.replaceAll(pattern, "|"+String.format("%.2f", normalRating)+"|");
		return lineTemp;
	}
}
