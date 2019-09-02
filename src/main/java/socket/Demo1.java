package socket;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Demo1 {

	public static void main(String[] args) {
		/*
		 * getByName()：通过域名获取ip地址
		 * getLocalHost：获取本机IP
		 */
		try {
			InetAddress address = InetAddress.getByName("www.baidu.com");
			System.out.println(address);
			
			InetAddress address2 = InetAddress.getByName("localhost");
			System.out.println(address2);
			
			InetAddress address4 = InetAddress.getByName("friendly");
			System.out.println(address4);
			
			InetAddress address3 = InetAddress.getLocalHost();
			System.out.println(address3);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
