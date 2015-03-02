package test.dimension;

import java.io.IOException;
import java.util.ArrayList;

import dimension.DimensionReduction;

public class Main {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length < 2){
			System.out.println("need input and output paths");
			System.exit(2);
		}
		DimensionReduction dr = new DimensionReduction();
		dr.setInputPath(args[0]);
		dr.setOutputPath(args[1]);
		dr.setInputFormat(MyInputFormat.class);
		ArrayList<Integer> indexes = new ArrayList<Integer>();
		indexes.add(0);
		indexes.add(1);
		dr.setDimToReserve(indexes);
		if(dr.run()){
			System.out.println("succeed to test if changed");
			System.exit(0);
		}else{
			System.out.println("something is wrong, please check your code");
			System.exit(2);
		}
	}

}
