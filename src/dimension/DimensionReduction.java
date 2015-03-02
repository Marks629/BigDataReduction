package dimension;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/*
 * this class provide dimension reduction algorithm
 * 
 * how to use:
 * DimensionReduction dr = new DimensionReduction();
 * dr.setInputPath(input);
 * dr.setOutputPath(output);
 * dr.setInputFormat(input);
 * output format is optional
 * dr.setDimToReserve(dim);
 * dr.run();//succeed to return true,else false
 * 
 * inputformat to implement:
 * key:anything
 * value:"dim1|dim2|dim3|dim4..."
 */
public class DimensionReduction {
	private String inputPath = null;
	private String outputPath = null;
	private ArrayList<Integer> dimToReserve = null;
	private Class inputFormat = null;
	private Class outputFormat = null;

	public void setInputPath(String input) {
		this.inputPath = input;
	}

	public void setOutputPath(String output) {
		this.outputPath = output;
	}

	public void setDimToReserve(ArrayList<Integer> al) {
		this.dimToReserve = al;
	}

	public void setInputFormat(Class input) {
		this.inputFormat = input;
	}

	public void setOutputFormat(Class output) {
		this.outputFormat = output;
	}

	public boolean run() throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration();
		if (inputPath == null || outputPath == null || inputFormat == null
				|| dimToReserve == null) {
			return false;
		}
		Job job = new Job(conf, "DimensionReduction");
		job.setJarByClass(DimensionReduction.class);
		job.setInputFormatClass(inputFormat);
		if (outputFormat != null)
			job.setOutputFormatClass(outputFormat);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		// pass parameters to maps
		job.getConfiguration().set("index_num",
				String.valueOf(dimToReserve.size()));
		for (int i = 0; i < dimToReserve.size(); ++i) {
			job.getConfiguration().set("index_" + i,
					dimToReserve.get(i).toString());
		}

		return job.waitForCompletion(true);
	}

	public static class TokenizerMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] indexes = value.toString().split("\\|");// |need to be dealed
															// specialy
			int indexNum = Integer.parseInt(context.getConfiguration().get(
					"index_num"));
			ArrayList<Integer> indexToReserve = new ArrayList<Integer>();
			for (int i = 0; i < indexNum; ++i) {
				indexToReserve.add(Integer.parseInt(context.getConfiguration()
						.get("index_" + i)));
			}
			String res = new String();

			if (indexNum != 0) {
				res += indexes[indexToReserve.get(0)];
			}
			for (int i = 1; i < indexNum; ++i) {
				res += "|" + indexes[indexToReserve.get(i)];
			}
			context.write(key, new Text(res));//yes here is a problem
			//context.write(key, new Text("hehe"));//not value problem
		}
	}

	public static class IntSumReducer extends
			Reducer<LongWritable, Text,Text, NullWritable> {

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(new Text(val.toString()+"hehe"), NullWritable.get());
			}
		}
	}
}
