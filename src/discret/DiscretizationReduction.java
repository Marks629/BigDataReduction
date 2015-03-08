package discret;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*
 * this class implements the discretization reduction algorithm
 * 
 * how to use:
 * DiscretizationReduction dr = new DiscretizationReduction();
 * dr.setInputPath(input);
 * dr.setOutputPath(output);
 * dr.setInputFormat(input);
 * dr.setDiscretInterval(0.02);
 * dr.run();true if succeeds,else false returned
 * 
 * inputformat:
 * key:DoubleWritable
 * value:DoubleWritable
 */
public class DiscretizationReduction {
	private String inputPath = null;
	private String outputPath = null;
	private Class inputFormat = null;
	private Class outputFormat = null;
	private double discretInterval = 1.0;
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
	public void setDiscretInterval(double interval){
		this.discretInterval = interval;
	}
	public boolean run() throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
	    if(inputPath == null || outputPath == null 
	    		|| inputFormat == null){
	    		return false;
	    }
	    Job job = new Job(conf, "discretization reduction");
	    job.setJarByClass(DiscretizationReduction.class);
	    job.setInputFormatClass(inputFormat);
	    if(outputFormat != null)job.setOutputFormatClass(outputFormat);
	    job.setMapperClass(MyMapper.class);
	    job.setCombinerClass(MyReducer.class);
	    job.setReducerClass(MyReducer.class);
	    job.setOutputKeyClass(DoubleWritable.class);
	    job.setOutputValueClass(DoubleWritable.class);
	    FileInputFormat.addInputPath(job, new Path(inputPath));
	    FileOutputFormat.setOutputPath(job, new Path(outputPath));
	    
	    //pass parameters to maps
	    job.getConfiguration().set("interval",String.valueOf(this.discretInterval));
	    
	    return job.waitForCompletion(true);
	}
	public static class MyMapper extends Mapper<DoubleWritable,DoubleWritable,DoubleWritable,DoubleWritable>{

		@Override
		protected void map(
				DoubleWritable key,
				DoubleWritable value,
				Mapper<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double interval = Double.parseDouble(context.getConfiguration().get("interval"));
			long tmp = (long) (key.get()/interval);
			double newkey = tmp*interval;
			context.write(new DoubleWritable(newkey), value);
		}
		
	}
	public static class MyReducer extends Reducer<DoubleWritable,DoubleWritable,DoubleWritable,DoubleWritable>{

		@Override
		protected void reduce(
				DoubleWritable arg0,
				Iterable<DoubleWritable> arg1,
				Reducer<DoubleWritable, DoubleWritable, DoubleWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			double sum  = 0.0;
			for(DoubleWritable dw:arg1){
				sum += dw.get();
			}
			context.write(arg0,new DoubleWritable(sum));
		}

	}
}
