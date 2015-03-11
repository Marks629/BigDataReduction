package test.mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Hello {

	public static class TokenizerMapper extends
			Mapper<Object, Text, IntWritable, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(one, word);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<IntWritable, Text, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(IntWritable key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		System.out.println(conf.get("fs.default.name"));
		if (args[0].startsWith("/")) {
			System.out.println("hehe");
		}
		System.out.println(conf.get("user.name"));
		System.exit(0);
		
		Job job = new Job(conf, "word count");
		job.setJarByClass(Hello.class);
		job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		// FileOutputFormat.setCompressOutput(job, true);
		// FileOutputFormat.setOutputCompressorClass(job,BZip2Codec.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
