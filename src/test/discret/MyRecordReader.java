package test.discret;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class MyRecordReader extends  RecordReader<DoubleWritable, DoubleWritable>{
	private LineRecordReader lrr = null;
	
	public MyRecordReader(){
		this.lrr = new LineRecordReader();
	}
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		lrr.close();
	}

	@Override
	public DoubleWritable getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] datas = lrr.getCurrentValue().toString().split(" ");
		return new DoubleWritable(Double.parseDouble(datas[0]));
	}

	@Override
	public DoubleWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		String[] datas = lrr.getCurrentValue().toString().split(" ");
		return new DoubleWritable(Double.parseDouble(datas[1]));
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lrr.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		lrr.initialize(arg0, arg1);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return lrr.nextKeyValue();
	}

}
