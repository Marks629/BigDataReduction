package test.java;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String test = "hello,world,ni hao zhong guo	ren	de	hao	ma	hehe";
		String arr[] = test.split(",| |\\\t");
		for(String a:arr){
			System.out.println(a);
		}
		String centers = "1.4279414489855558 11.642577778433282 2.669783559697353 2.2910584526611855 13.238632100933373 158.77897336977486 1.9458404243469434 5.066617420930118 0.2539804825841683 1.9526510130238013 5.706001200382435 1.5860658298440213 2.242967663563112 1638.7671101934045";
		String newCenters = "1.025, 13.71375, 1.8682500000000002, 2.435, 17.2125, 104.65, 2.8497500000000002, 3.0010000000000003, 0.27024999999999993, 1.8992499999999999, 5.629, 1.07525, 3.1672499999999997, 1205.8";
		for(String s :centers.split(",| |\\\t|, | ,| , ")){
			System.out.println(s);
		}
		for(String s:newCenters.split(",| |\\\t|, | ,| , ")){
			System.out.println(s);
		}
	}

}
