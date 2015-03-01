package dataCube;

import java.io.IOException;
import java.util.ArrayList;
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

/*
 * this class provide datacube reduction algorithm
 * 
 * how to use:
 * DataCubeReduction dcr = new DataCubeReduction();
 * dcr.setInputPath("path to input");
 * dcr.setOutputPath("path to output");
 * dcr.setInputFormat(myInputFormat.class);
 * dcr.setIndexToReserve(arraylist<int>);
 * //OutputFormat is optional
 * boolean ret = dcr.run();//true if succeed,else false
 * 
 * InputFormat to implement:
 * key:"index1|index2|index3|index4..."(Text)
 * value:123.456(DoubleWritable)
 */
public class DataCubeReduction {
	private String inputPath = null;
	private String outputPath = null;
	private Class inputFormat = null;
	private Class outputFormat = null;
	private ArrayList<Integer> indexToReserve = null;
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
	public void setIndexToReserve(ArrayList<Integer> al){
		this.indexToReserve = al;
	}
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
	    if(inputPath == null || outputPath == null 
	    		|| inputFormat == null || indexToReserve == null){
	    		return false;
	    }
	    Job job = new Job(conf, "dataCube");
	    job.getConfiguration().set("hehehe", "hahaha");
	    job.setJarByClass(DataCubeReduction.class);
	    job.setInputFormatClass(inputFormat);
	    if(outputFormat != null)job.setOutputFormatClass(outputFormat);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
	    //pass parameters to maps
	    job.getConfiguration().set("index_num", String.valueOf(indexToReserve.size()));
	    for(int i = 0; i < indexToReserve.size(); ++i){
	    		job.getConfiguration().set("index_"+i, indexToReserve.get(i).toString());
	    }
	    
	    return job.waitForCompletion(true);
	}
	public static class TokenizerMapper extends
			Mapper<Text, DoubleWritable, Text, DoubleWritable> {

		public void map(Text key, DoubleWritable value, Context context)
				throws IOException, InterruptedException {
			String[] indexes = key.toString().split("\\|");// |need to be dealed specialy
			int indexNum = Integer.parseInt(context.getConfiguration().get("index_num"));
			ArrayList<Integer> indexToReserve = new ArrayList<Integer>();
			for(int i = 0; i < indexNum; ++i){
				indexToReserve.add(
						Integer.parseInt(context.getConfiguration().get("index_"+i)));
			}
			String res = new String();
			
			if(indexNum != 0){
				res += indexes[indexToReserve.get(0)];
			}
			for(int i = 1; i < indexNum; ++i){
				res += "|" + indexes[indexToReserve.get(i)];
			}
			context.write(new Text(res), value);
		}
	}

	public static class IntSumReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0.0;
			for (DoubleWritable val : values) {
				sum += val.get();
			}
			context.write(key,new DoubleWritable(sum));
		}
	}
}
