// A Java program for a Client
import java.net.*;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;

public class Client {

    public static void main(String args[]) {
        BufferedReader sysReadIn = new BufferedReader(new InputStreamReader(System.in));
        Set<String> stateSet = new HashSet<>();
        String clientName = args[0];
        int requestNum = 0;

        for (int i = 1; i < args.length; i++) {
            String input = args[i];
            int port = Integer.parseInt(input);
            ServerHandler server = new ServerHandler("127.0.0.1", port, "Connection request", stateSet);
            Thread t = new Thread(server);
            t.start();
        }

        // keep reading until "Over" is input
        String line = "";
        while (!line.equals("Over")) {
            try {
                line = sysReadIn.readLine();
                if (line == null || line.equals("")) continue;
                requestNum++;
                StringBuilder sb = new StringBuilder();
                sb.append(clientName);
                sb.append(" ");
                sb.append(requestNum);
                sb.append("|");
                sb.append(line);
                line = sb.toString();

                for (int i = 1; i < args.length; i++) {
                    String input = args[i];
                    int port = Integer.parseInt(input);
                    ServerHandler server = new ServerHandler("127.0.0.1", port, line, stateSet);
                    Thread t = new Thread(server);
                    t.start();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            sysReadIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static class ServerHandler extends Thread {
        String address;
        int port;
        String message;
        Set<String> stateSet;

        public ServerHandler(String address, int port, String s, Set<String> set) {
            this.address = address;
            this.port = port;
            this.message = s;
            this.stateSet = set;
        }

        @Override
        public void run() {
            // establish a connection
            try {
                Socket socket = new Socket(address, port);
                DataInputStream in = new DataInputStream(socket.getInputStream());
                DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                out.writeUTF("C::Connection request");
                String line = in.readUTF();
                System.out.println(line);
                String[] received = line.split(" ");
                int isReady = Integer.parseInt(received[received.length - 1]);
                if (!message.equals("Connection request")) {
                    out.writeUTF("C::" + message);
                    if (isReady == 1) {
                        received = in.readUTF().split(":");
                        synchronized (stateSet) {
                            if (stateSet.contains(received[1])) {
                                System.out.println("Discarded duplicate reply from " + received[0]);
                            } else {
                                System.out.println("<" + received[0] + "> received:" + received[1]);
                                stateSet.add(received[1]);
                            }
                        }
                    }
                }

            } catch (UnknownHostException e) {
                //e.printStackTrace();
                System.out.println("Can't connect to server at " + port);
            } catch (IOException e) {
                System.out.println("Lost connection to server at " + port);
                //e.printStackTrace();
            }
        }

    }
}