package NBC_lisan;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Main_CalNums {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Job job = new Job();
		job.setJarByClass(Main_CalNums.class);
		job.setJobName("CalNums");
		
		Path inputPath = new Path("BDAnalysis/exp2/NBC/Input/SUSYTrainSmall.txt");
		Path outputPath = new Path("BDAnalysis/exp2/NBC/Output/attriNumsByC/");
		Configuration conf = job.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
        if(fs.exists(outputPath))
        {
            fs.delete(outputPath, true);
        }
        
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		MultipleOutputs.addNamedOutput(job, "classificationsNum", TextOutputFormat.class, Text.class, IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Mapper_calNums.class);
		job.setReducerClass(Reducer_calNums.class);
		
		System.exit(job.waitForCompletion(true)?0:1);

	}

}
