package common.messages;

import java.net.InetAddress;



public class Testong {

	public static void main(String[] args){
		try{
		System.out.println(InetAddress.getLocalHost().getHostAddress());
		}catch(Exception e){}
	}
}
