package ngrammr;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final int MAX_N = 9;
	private static int GRAM_N;
	private static boolean OVERRIDE_N_LIMIT;

	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		GRAM_N = Integer.parseInt(conf.get("n", "5"));
		System.out.println("the N for the ngram is: " + conf.get("n"));
		OVERRIDE_N_LIMIT = Boolean.parseBoolean(conf.get("override", "false"));

		if (GRAM_N > MAX_N && OVERRIDE_N_LIMIT == false) {
			throw new IllegalArgumentException("Gram N exceeds Max N limit. "
					+ "Max n=9 or set override=true. See documentation");
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		ArrayList<String> grams = NGramFactory.grams(value.toString(), GRAM_N);

		for (String gram : grams) {
			context.write(new Text(gram), new IntWritable(1));
		}
	}
}
