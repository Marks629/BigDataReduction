package clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class InitializeKmean {
	private String outputPath = null;
	private String inputPath = null;
	private Class inputFormat = null;
	private int k = -1;

	public void setInputPath(String input) {
		this.inputPath = input;
	}

	public void setOutputPath(String output) {
		this.outputPath = output;
	}

	public void setNumberOfCenters(int k) {
		this.k = k;
	}

	public void setInputFormat(Class format) {
		this.inputFormat = format;
	}

	public boolean run() throws IOException, ClassNotFoundException,
			InterruptedException {
		Configuration conf = new Configuration();
		if (this.inputPath == null || this.outputPath == null || this.k < 0
				|| this.inputFormat == null) {
			System.out.println("some params need to be set");
			System.exit(-1);
		}
		Job job = new Job(conf, "InitializeKmean");
		job.setJarByClass(InitializeKmean.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(DoubleWritable.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(this.inputPath));
		FileOutputFormat.setOutputPath(job, new Path(this.outputPath));
		boolean ret = job.waitForCompletion(true);
		generateCenters(job.getConfiguration().get("fs.default.name")
				+ outputPath, k);
		return ret;
	}

	private void generateCenters(String centerPath, int k) throws IOException {
		ArrayList<ArrayList<Double>> limits = Utils.getCentersFromHDFS(
				centerPath, true);
		Utils.deletePath(centerPath);
		Random random = new Random();
		Configuration conf = new Configuration();
		Path outPath = new Path(centerPath + "/centers");
		FileSystem fileSystem = outPath.getFileSystem(conf);
		FSDataOutputStream overWrite = fileSystem.create(outPath, true);
		for (int i = 0; i < k; ++i) {
			String center = new String();
			for (int j = 0; j < limits.size(); ++j) {
				double estimate = limits.get(j).get(0) + random.nextDouble()
						* (limits.get(j).get(1) - limits.get(j).get(0));
				if (j == 0) {
					center += String.valueOf(estimate);
				} else {
					center += " ";
					center += String.valueOf(estimate);
				}
			}
			center += "\n";
			// overWrite.writeUTF(center);
			overWrite.writeBytes(center);
		}
		overWrite.close();
	}

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, DoubleWritable> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			ArrayList<Double> dimens = Utils.textToArray(value);
			for (int i = 0; i < dimens.size(); ++i) {
				context.write(new IntWritable(i),
						new DoubleWritable(dimens.get(i)));
			}
		}
	}

	public static class Reduce
			extends
			Reducer<IntWritable, DoubleWritable, DoubleWritable, DoubleWritable> {
		@Override
		protected void reduce(
				IntWritable key,
				Iterable<DoubleWritable> values,
				Reducer<IntWritable, DoubleWritable, DoubleWritable, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			Double min = Double.MAX_VALUE;
			Double max = Double.MIN_VALUE;
			for (DoubleWritable d : values) {
				if (d.get() < min) {
					min = d.get();
				}
				if (d.get() > max) {
					max = d.get();
				}
			}
			context.write(new DoubleWritable(min), new DoubleWritable(max));
		}
	}
}
