package clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
 * KeamnClustering implements the kmeans clustering algorithm
 * 
 * how to use:
 * KmeanClustering kc = new KmeanClustering();
 * kc.setInputPath(input);
 * kc.setOutputPath(output);
 * kc.setK(k);
 * kc.setInputFormat(format);
 * kc.run();
 * 
 * inputformat:
 * key:anything(LongWritable)
 * value:123,234,345,456,567,123(Text) //or use space to split
 */
public class KmeanClustering {
	private String inputPath = null;
	private String outputPath = null;
	private Class inputFormat = null;
	private int k = -1;
	private String tmpPath = "/qewradsfzxcv413";

	public void setInputPath(String input) {
		this.inputPath = input;
	}

	public void setOutputPath(String output) {
		this.outputPath = output;
	}

	public void setInputFormat(Class format) {
		this.inputFormat = format;
	}

	public void setK(int k) {
		this.k = k;
	}

	public boolean runCluster() throws ClassNotFoundException, IOException,
			InterruptedException {
		if (this.inputPath == null || this.outputPath == null
				|| this.inputFormat == null || this.k < 0) {
			System.out.println("params needed");
			System.exit(-1);
		}
		// generate initial centers
		InitializeKmean ik = new InitializeKmean();
		ik.setInputPath(this.inputPath);
		ik.setOutputPath(this.outputPath);
		ik.setNumberOfCenters(this.k);
		ik.setInputFormat(this.inputFormat);
		ik.run();

		// run kmeans
		String centerPath = this.outputPath;
		String dataPath = this.inputPath;
		String newCenterPath = this.tmpPath;

		int count = 0;// couters
		while (true) {
			KmeanClustering.run(centerPath, dataPath, newCenterPath,
					this.inputFormat, true);
			System.out.println("iteration " + ++count);
			if (Utils.compareCenters(centerPath, newCenterPath)) {
				KmeanClustering.run(centerPath, dataPath, newCenterPath,
						this.inputFormat, false);
				Utils.deletePath(newCenterPath);
				break;
			}
		}
		return true;
	}

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, Text> {

		// 中心集合
		ArrayList<ArrayList<Double>> centers = null;
		// 用k个中心
		int k = 0;

		// 读取中心
		protected void setup(Context context) throws IOException,
				InterruptedException {
			centers = Utils.getCentersFromHDFS(
					context.getConfiguration().get("centersPath"), true);
			k = centers.size();
		}

		/**
		 * 1.每次读取一条要分类的条记录与中心做对比，归类到对应的中心 2.以中心ID为key，中心包含的记录为value输出(例如： 1 0.2
		 * 。 1为聚类中心的ID，0.2为靠近聚类中心的某个值)
		 */
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// 读取一行数据
			ArrayList<Double> fileds = Utils.textToArray(value);
			int sizeOfFileds = fileds.size();

			double minDistance = 99999999;
			int centerIndex = 0;

			// 依次取出k个中心点与当前读取的记录做计算
			for (int i = 0; i < k; i++) {
				double currentDistance = 0;
				for (int j = 0; j < sizeOfFileds; j++) {
					double centerPoint = Math.abs(centers.get(i).get(j));
					double filed = Math.abs(fileds.get(j));
					currentDistance += Math.pow((centerPoint - filed)
							/ (centerPoint + filed), 2);
				}
				// 循环找出距离该记录最接近的中心点的ID
				if (currentDistance < minDistance) {
					minDistance = currentDistance;
					centerIndex = i;
				}
			}
			// 以中心点为Key 将记录原样输出
			context.write(new IntWritable(centerIndex + 1), value);
		}

	}

	// 利用reduce的归并功能以中心为Key将记录归并到一起
	public static class Reduce extends
			Reducer<IntWritable, Text, NullWritable, Text> {

		/**
		 * 1.Key为聚类中心的ID value为该中心的记录集合 2.计数所有记录元素的平均值，求出新的中心
		 */
		protected void reduce(IntWritable key, Iterable<Text> value,
				Context context) throws IOException, InterruptedException {
			ArrayList<ArrayList<Double>> filedsList = new ArrayList<ArrayList<Double>>();

			// 依次读取记录集，每行为一个ArrayList<Double>
			for (Iterator<Text> it = value.iterator(); it.hasNext();) {
				ArrayList<Double> tempList = Utils.textToArray(it.next());
				filedsList.add(tempList);
			}

			// 计算新的中心
			// 每行的元素个数
			int filedSize = filedsList.get(0).size();
			double[] avg = new double[filedSize];
			for (int i = 0; i < filedSize; i++) {
				// 求没列的平均值
				double sum = 0;
				int size = filedsList.size();
				for (int j = 0; j < size; j++) {
					sum += filedsList.get(j).get(i);
				}
				avg[i] = sum / size;
			}
			context.write(NullWritable.get(), new Text(Arrays.toString(avg)
					.replace("[", "").replace("]", "").replace(" ", "")));
		}

	}

	@SuppressWarnings("deprecation")
	public static void run(String centerPath, String dataPath,
			String newCenterPath, Class inputFormat, boolean runReduce)
			throws IOException, ClassNotFoundException, InterruptedException {

		Configuration conf = new Configuration();
		conf.set("centersPath", conf.get("fs.default.name") + centerPath);

		Job job = new Job(conf, "mykmeans");
		job.setJarByClass(KmeanClustering.class);

		job.setMapperClass(Map.class);

		job.setInputFormatClass(inputFormat);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		if (runReduce) {
			// 最后依次输出不许要reduce
			job.setReducerClass(Reduce.class);
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
		}

		FileInputFormat.addInputPath(job, new Path(conf.get("fs.default.name")
				+ dataPath));

		FileOutputFormat.setOutputPath(job,
				new Path(conf.get("fs.default.name") + newCenterPath));

		System.out.println(job.waitForCompletion(true));
	}

}