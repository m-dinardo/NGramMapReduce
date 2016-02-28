package ngrammr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class NGramDriver extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = getConf();
		Job job = Job.getInstance(conf);

		job.setJarByClass(NGramDriver.class);
		job.setMapperClass(NGramMapper.class);
		job.setReducerClass(NGramReducer.class);
		job.setCombinerClass(NGramReducer.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new NGramDriver(), args);
		System.exit(exitCode);
	}
}
