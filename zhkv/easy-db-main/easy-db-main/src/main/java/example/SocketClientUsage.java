/*
 *@Type SocketClientUsage.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 14:07
 * @version
 */
package example;

import client.Client;
import client.SocketClient;

public class SocketClientUsage {
    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        Client client = new SocketClient(host, port);

//        client.set("键2", "值2");
        System.out.println(client.get("键2")); // 应该输出 value1
//        client.rm("键2");
//        System.out.println(client.get("键1")); // 应该输出 null 或者空值
    }
}
