package exp1_DateSimple;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DSimple_Mapper extends Mapper<LongWritable, Text, Text, Text> {

	/**
	 * map类
	 * 输入:文件中的行信息,其中每一行为 旅游者对POI的评价数据
	 *     key为偏移量
	 *     value为每一行的内容
	 * 输出:key:每一行数据的user_career(使用者职业)属性信息
	 *     value:文件中一行的数据内容;
	 * 
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String user_career = line.split("\\|")[10]; //提取职业属性信息
		
		Text tCareer = new Text();
		Text tData = new Text();
		tCareer.set(user_career);
		tData.set(line);
		
		context.write(tCareer, tData);

	}
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		
	}
}
