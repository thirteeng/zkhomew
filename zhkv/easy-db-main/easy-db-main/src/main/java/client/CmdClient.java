/*
 *@Type CmdClient.java
 * @Desc
 * @Author urmsone urmsone@163.com
 * @date 2024/6/13 13:58
 * @version
 */
package client;

import java.util.Scanner;

public class CmdClient {
    private Client client;

    public CmdClient(Client client) {
        this.client = client;
    }

    public void start() {
        Scanner scanner = new Scanner(System.in);
        String command;
        while (true) {
            System.out.print("> ");
            command = scanner.nextLine();
            String[] parts = command.split("\\s+");
            if (parts.length == 0) continue;

            String action = parts[0];
            if (action.equalsIgnoreCase("exit")) {
                break;
            } else if (action.equalsIgnoreCase("set") && parts.length == 3) {
                String key = parts[1];
                String value = parts[2];
                client.set(key, value);
            } else if (action.equalsIgnoreCase("get") && parts.length == 2) {
                String key = parts[1];
                String value = client.get(key);
                System.out.println("Value: " + value);
            } else if (action.equalsIgnoreCase("rm") && parts.length == 2) {
                String key = parts[1];
                client.rm(key);
            } else {
                System.out.println("Invalid command. Use set <key> <value>, get <key>, rm <key>, or exit.");
            }
        }
        scanner.close();
    }

    public static void main(String[] args) {
        String host = "localhost";
        int port = 12345;
        Client client = new SocketClient(host, port);
        CmdClient cmdClient = new CmdClient(client);
        cmdClient.start();
    }
}
