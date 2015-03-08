package test.discret;

import java.io.IOException;
import java.util.ArrayList;

import dimension.DimensionReduction;
import discret.DiscretizationReduction;
public class Main {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		// TODO Auto-generated method stub
		if(args.length < 2){
			System.out.println("need input and output paths");
			System.exit(2);
		}
		DiscretizationReduction dr = new DiscretizationReduction();
		dr.setInputPath(args[0]);
		dr.setOutputPath(args[1]);
		dr.setInputFormat(MyInputFormat.class);
		dr.setDiscretInterval(0.1);
		if(dr.run()){
			System.out.println("succeed to test if changed");
			System.exit(0);
		}else{
			System.out.println("something is wrong, please check your code");
			System.exit(2);
		}
	}

}
