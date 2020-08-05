package NBC_lisan;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Reducer_calNums extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	//将结果输出到多个文件
	private MultipleOutputs<Text, IntWritable> mos;
	//创建对象
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos = new MultipleOutputs<Text,IntWritable>(context);
	}
	/**
	 * reduce类
	 * 输入:经过shuffle的过程,输入的键值对为
	 *     key:属性值user_career
	 *     value:该属性所以应的一条数据信息
	 * 输出:按1%进行分成抽样的数据信息
	 *     key:null
	 *     value:用户数据信息
	 * @throws InterruptedException 
	 * @throws IOException 
	 * 
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		Iterator<IntWritable> iter = values.iterator();
		int value = 0;
		int sum = 0;
		while(iter.hasNext()) {
			value = iter.next().get();
			sum += value;
		}
		if(key.toString().equals("T")||key.toString().equals("F")) {
			mos.write("classificationsNum", key, sum);
		}else {
			context.write(key, new IntWritable(sum));
		}		
	}
	
	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		mos.close();
	}
}
