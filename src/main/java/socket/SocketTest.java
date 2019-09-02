package socket;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.junit.Test;


public class SocketTest {

	@Test
	public void client() throws UnknownHostException{
		Socket socket = null;
		OutputStream os = null;
		InetAddress inet = InetAddress.getByName("10.1.4.33");
		try {
			//创建socket对象
			socket = new Socket(inet, 9998);
			//获取输出流
			os = socket.getOutputStream();
			//写出数据
			os.write("你好，我是客户端。。。。。。。".getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(os != null){
					os.close();
				}
			} catch (Exception e2) {
				// TODO: handle exception
			}
		}
	}
	
	@Test
	public void server(){
		//创建ServerSocket对象
		ServerSocket sSocket = null;
		Socket socket = null;
		ByteArrayOutputStream baos = null;
		InputStream is = null;
		try {
			sSocket = new ServerSocket(9998);
			socket = sSocket.accept();
			is = socket.getInputStream();
			baos = new ByteArrayOutputStream();
			byte[] buffer = new byte[5];
			int len;
			while((len = is.read(buffer)) != -1){
				baos.write(buffer,0,len);
			}
			System.out.println(baos.toString());
			System.out.println("客户端主机：" + socket.getInetAddress().getHostAddress());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try{
				if(is != null){
					is.close();
				}
				if(socket != null){
					socket.close();
				}
				if(sSocket != null){
					sSocket.close();
				}
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}
}
