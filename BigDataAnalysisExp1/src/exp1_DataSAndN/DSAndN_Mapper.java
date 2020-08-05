package exp1_DataSAndN;

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

public class DSAndN_Mapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
	@Override
	/**
	 * map类
	 * 输入:文件中的行信息,其中每一行为 旅游者对POI的评价数据
	 *     key为偏移量
	 *     value为每一行的内容
	 * 输出:key:对属性review_date,user_birthday,temperature进行标准化以及对rating进行归一化之后的一条数据
	 *     value:null
	 * 
	 */
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String review_date = line.split("\\|")[4];
		String user_birthday = line.split("\\|")[8];
		String temperature = line.split("\\|")[5];
		String rating = line.split("\\|")[6];
		
		String lineTemp1 = null;
		String lineTemp2 = null;
		try {
			lineTemp1 = standardizingDate(line, review_date);
			lineTemp2 = standardizingDate(lineTemp1, user_birthday);
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		String lineTemp3 = standardizingTem(lineTemp2, temperature);
		String lineTemp4 = normalizingRating(context, lineTemp3, rating);
		
		Text text = new Text();
		text.set(lineTemp4);
		context.write(text, NullWritable.get());
		
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
		double max = context.getConfiguration().getDouble("max", 0);
		double min = context.getConfiguration().getDouble("min", 0);
		double normalRating = (rating - min)/(max - min);
		
		String pattern = "\\|[\\d]+.[\\d]{2}\\|";
		String lineTemp = line.replaceAll(pattern, "|"+String.format("%.2f", normalRating)+"|");
		return lineTemp;
	}

}
