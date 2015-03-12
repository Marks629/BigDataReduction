package clustering;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class Utils {

	// 读取中心文件的数据
	public static ArrayList<ArrayList<Double>> getCentersFromHDFS(
			String centersPath, boolean isDirectory) throws IOException {

		ArrayList<ArrayList<Double>> result = new ArrayList<ArrayList<Double>>();

		Path path = new Path(centersPath);

		Configuration conf = new Configuration();

		FileSystem fileSystem = path.getFileSystem(conf);

		if (isDirectory) {
			FileStatus[] listFile = fileSystem.listStatus(path);
			for (int i = 0; i < listFile.length; i++) {
				result.addAll(getCentersFromHDFS(listFile[i].getPath()
						.toString(), false));
			}
			return result;
		}
		
		//判断是否是系统默认输出的_logs and _SUCCESS
		String tmp[] = centersPath.split("/");
		if(tmp[tmp.length-1].startsWith("_")){
			return result;
		}

		FSDataInputStream fsis = fileSystem.open(path);
		LineReader lineReader = new LineReader(fsis, conf);

		Text line = new Text();

		while (lineReader.readLine(line) > 0) {
			ArrayList<Double> tempList = textToArray(line);
			result.add(tempList);
		}
		lineReader.close();
		return result;
	}

	// 删掉文件
	public static void deletePath(String pathStr) throws IOException {
		Configuration conf = new Configuration();
		Path path = new Path(pathStr);
		FileSystem hdfs = path.getFileSystem(conf);
		hdfs.delete(path, true);
	}

	public static ArrayList<Double> textToArray(Text text) {
		ArrayList<Double> list = new ArrayList<Double>();
		String[] fileds = text.toString().split(",| |\\\t|, | ,| , ");
		for (int i = 0; i < fileds.length; i++) {
			list.add(Double.parseDouble(fileds[i]));
		}
		return list;
	}

	public static boolean compareCenters(String centerPath, String newPath)
			throws IOException {

		Configuration conf = new Configuration();
		
		List<ArrayList<Double>> oldCenters = Utils.getCentersFromHDFS(
				conf.get("fs.default.name")+centerPath, true);
		List<ArrayList<Double>> newCenters = Utils.getCentersFromHDFS(conf.get("fs.default.name")+newPath,
				true);

		int size = oldCenters.size();
		int fildSize = oldCenters.get(0).size();
		double distance = 0;
		for (int i = 0; i < size; i++) {
			for (int j = 0; j < fildSize; j++) {
				double t1 = Math.abs(oldCenters.get(i).get(j));
				double t2 = Math.abs(newCenters.get(i).get(j));
				distance += Math.pow((t1 - t2) / (t1 + t2), 2);
			}
		}

		if (distance == 0.0) {
			// 删掉新的中心文件以便最后依次归类输出
			Utils.deletePath(newPath);
			return true;
		} else {
			// 先清空中心文件，将新的中心文件复制到中心文件中，再删掉中心文件

			Path outPath = new Path(centerPath+"/centers");
			FileSystem fileSystem = outPath.getFileSystem(conf);

			//FSDataOutputStream overWrite = fileSystem.create(outPath, true);
			//overWrite.writeChars("");
			//overWrite.close();
			Utils.deletePath(centerPath);

			Path inPath = new Path(newPath);
			FileStatus[] listFiles = fileSystem.listStatus(inPath);
			for (int i = 0; i < listFiles.length; i++) {
				//skip _log and _SUCCESS
				String path = listFiles[i].getPath().toString();
				String tmp[] = path.split("/");
				if(tmp[tmp.length-1].startsWith("_")){
					continue;
				}
				
				FSDataOutputStream out = fileSystem.create(outPath);
				FSDataInputStream in = fileSystem.open(listFiles[i].getPath());
				IOUtils.copyBytes(in, out, 4096, true);
			}
			// 删掉新的中心文件以便第二次任务运行输出
			Utils.deletePath(newPath);
		}

		return false;
	}
}
