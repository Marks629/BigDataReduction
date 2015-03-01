package test.dataCube;

import java.io.IOException;
import java.util.ArrayList;

import dataCube.DataCubeReduction;

public class Main {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length < 2){
			System.out.println("need input and output paths");
			System.exit(2);
		}
		DataCubeReduction dcr = new DataCubeReduction();
		dcr.setInputPath(args[0]);
		dcr.setOutputPath(args[1]);
		dcr.setInputFormat(MyInputFormat.class);
		ArrayList<Integer> indexes = new ArrayList<Integer>();
		indexes.add(0);
		indexes.add(1);
		dcr.setIndexToReserve(indexes);
		if(dcr.run()){
			System.out.println("succeed");
			System.exit(0);
		}else{
			System.out.println("something is wrong, please check your code");
			System.exit(2);
		}
	}

}
