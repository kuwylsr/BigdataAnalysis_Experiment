package exp1_DataFilter;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DFindLimit_Mapper extends Mapper<LongWritable, Text, DoubleWritable, Text>{

	
	
	@Override
	/**
	 * map类
	 * 输入:文件中的行信息,其中每一行为 旅游者对POI的评价数据
	 *     key为偏移量
	 *     value为每一行的内容
	 * 输出:key:每一行数据的rating(用户评分)属性信息,(若评分为缺失值,则按数值-1传递给reducer)
	 *     value:文件中一行的数据内容;
	 * 
	 */
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String rating = line.split("\\|")[6];
		
		Text tData = new Text();
		tData.set(line);
		if(!rating.equals("?")) {
			DoubleWritable tRating = new DoubleWritable();
			tRating.set(Double.parseDouble(rating));
			context.write(tRating, tData);
		}else {
			DoubleWritable defectRating = new DoubleWritable();
			defectRating.set(-1);
			context.write(defectRating, tData);
		}
	}
}
