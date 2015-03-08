package test.discret;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MyInputFormat extends FileInputFormat<DoubleWritable,DoubleWritable>{

	@Override
	public RecordReader<DoubleWritable, DoubleWritable> createRecordReader(InputSplit arg0,
			TaskAttemptContext arg1) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return new MyRecordReader();
	}

}
