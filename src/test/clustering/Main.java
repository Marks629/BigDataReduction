package test.clustering;

import java.io.IOException;

import clustering.InitializeKmean;
import clustering.KmeanClustering;
import clustering.Utils;

public class Main {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		if(args.length < 3){
			System.out.println("need input output tmp dirs");
			System.exit(-1);
		}
		
		// generate initial centers
		InitializeKmean ik = new InitializeKmean();
		ik.setInputPath(args[0]);
		ik.setOutputPath(args[1]);
		ik.setNumberOfCenters(5);
		ik.run();

		// run kmeans
		String centerPath = args[1];
		String dataPath = args[0];
		String newCenterPath = args[2];

		int count = 0;// couters
		while (true) {
			KmeanClustering.run(centerPath, dataPath, newCenterPath, true);
			System.out.println(" 第 " + ++count + " 次计算 ");
			if (Utils.compareCenters(centerPath, newCenterPath)) {
				KmeanClustering.run(centerPath, dataPath, newCenterPath, false);
				Utils.deletePath(newCenterPath);
				break;
			}
		}
	}

}
