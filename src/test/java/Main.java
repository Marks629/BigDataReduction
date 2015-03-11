package test.java;

public class Main {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String test = "hello,world,ni hao zhong guo	ren	de	hao	ma	hehe";
		String arr[] = test.split(",| |\\\t");
		for(String a:arr){
			System.out.println(a);
		}
	}

}
