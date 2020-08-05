package NBC_lisan;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper_calNums extends Mapper<LongWritable, Text, Text, IntWritable>{

	
	/**
	 * map类
	 * 输入:文件中的行信息,其中每一行为 旅游者对POI的评价数据
	 *     key为偏移量
	 *     value为每一行的内容
	 * 输出:key:每一行数据的user_career(使用者职业)属性信息
	 *     value:文件中一行的数据内容;
	 * 
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		String[] attributions = line.split("\t");
		String temp = "";
		Text t = new Text();
		String classification = attributions[0];
		for(int i = 1 ;i<attributions.length;i++) {
			temp = attributions[i] + "," + classification;
			t.set(temp);
			context.write(t, new IntWritable(1));
		}
		if(Double.parseDouble(classification) == 1) {
			t.set("F");
			context.write(t, new IntWritable(1));
		}else {
			t.set("T");
			context.write(t, new IntWritable(1));
		}
	}
}
