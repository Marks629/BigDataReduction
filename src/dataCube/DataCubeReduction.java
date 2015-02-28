package dataCube;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataCubeReduction {
	private String inputPath = null;
	private String outputPath = null;
	private Class inputFormat = null;
	private Class outputFormat = null;
	
	public void setInputPath(String input){
		this.inputPath = input;
	}
	public void setOutputPath(String output){
		this.outputPath = output;
	}
	public void setInputFormat(Class input){
		this.inputFormat = input;
	}
	public void setOutputFormat(Class output){
		this.outputFormat = output;
	}
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
	    if(inputPath == null || outputPath == null || inputFormat == null){
	    		return false;
	    }
	    Job job = new Job(conf, "dataCube");
	    job.setJarByClass(DataCubeReduction.class);
	    job.setInputFormatClass(inputFormat);
	    if(outputFormat != null)job.setOutputFormatClass(outputFormat);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    return job.waitForCompletion(true);
	}
	public static class TokenizerMapper extends
			Mapper<Text, Text, Text, DoubleWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(key.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				//context.write(word, one);
			}
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			//context.write(key, result);
		}
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
