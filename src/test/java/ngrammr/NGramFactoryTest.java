package ngrammr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.junit.Before;
import org.junit.Test;

public class NGramFactoryTest {
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
	
	@Before
	public void setUp() {
		NGramMapper mapper = new NGramMapper();
		NGramReducer reducer = new NGramReducer();
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>();
		mapReduceDriver.setMapper(mapper);
		mapReduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(1), new Text("to be or to"));
		mapDriver.withOutput(new Text("to be or to"), new IntWritable(1));
		mapDriver.withOutput(new Text("to be or"), new IntWritable(1));
		mapDriver.withOutput(new Text("be or to"), new IntWritable(1));
		mapDriver.withOutput(new Text("to be"), new IntWritable(1));
		mapDriver.withOutput(new Text("be or"), new IntWritable(1));
		mapDriver.withOutput(new Text("or to"), new IntWritable(1));
		mapDriver.withOutput(new Text("to"), new IntWritable(1));
		mapDriver.withOutput(new Text("be"), new IntWritable(1));
		mapDriver.withOutput(new Text("or"), new IntWritable(1));
		mapDriver.withOutput(new Text("to"), new IntWritable(1));
		
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("to"), values);
		reduceDriver.withOutput(new Text("to"), new IntWritable(2));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("to be or to"));
		mapDriver.withOutput(new Text("to be or to"), new IntWritable(1));
		mapDriver.withOutput(new Text("to be or"), new IntWritable(1));
		mapDriver.withOutput(new Text("be or to"), new IntWritable(1));
		mapDriver.withOutput(new Text("to be"), new IntWritable(1));
		mapDriver.withOutput(new Text("be or"), new IntWritable(1));
		mapDriver.withOutput(new Text("or to"), new IntWritable(1));
		mapDriver.withOutput(new Text("to"), new IntWritable(2));
		mapDriver.withOutput(new Text("be"), new IntWritable(1));
		mapDriver.withOutput(new Text("or"), new IntWritable(1));
	}
}
