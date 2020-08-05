package NBC_lisan;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

public class Main_BayesClassify {

	public static void main(String[] args) throws IOException {
		Job job = new Job();
		job.setJarByClass(Main_BayesClassify.class);
		job.setJobName("Classify");
		
		Path inputPath = new Path("BDAnalysis/exp2/NBC/Input/SUSYTestSmall.txt");
		Path outputPath = new Path("BDAnalysis/exp2/NBC/Output/");

	}

}
