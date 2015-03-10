package compress;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CompressFactory {
	public static void setMapCompressor(Job job,Class compressor){
		job.getConfiguration().setBoolean("mapred.compress.map.output",true);
		job.getConfiguration().setClass("mapred.map.output.compression.codec", compressor, 
             CompressionCodec.class);
	}
	public static void setReduceCompressor(Job job,Class compressor){
		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, compressor);
	}
}
