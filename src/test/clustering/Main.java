package test.clustering;

import java.io.IOException;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import clustering.KmeanClustering;

public class Main {

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		if (args.length < 3) {
			System.out.println("need input output tmp dirs");
			System.exit(-1);
		}

		KmeanClustering kc = new KmeanClustering();
		kc.setInputPath(args[0]);
		kc.setOutputPath(args[1]);
		kc.setInputFormat(TextInputFormat.class);
		kc.setK(Integer.parseInt(args[2]));

		if (kc.runCluster()) {
			System.out.println("cluster succeed");
		} else {
			System.out.println("failed to cluster");
		}
	}

}
