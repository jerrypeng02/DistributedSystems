import java.io.*;
import java.net.*;
import java.util.*;

public class GFD {
    private static ServerSocket gfd;
    private static Socket rm;
    private static int connID = 1;
    private static List<String> serverList = new LinkedList<>();
    private volatile static int dsState = -1;

    public static void startGFD() throws IOException {
        boolean isConn = false;
        DataOutputStream rmStream = null;
        while (!isConn) {
            try {
                rm = new Socket("127.0.0.1", 6000);
                rmStream = new DataOutputStream(rm.getOutputStream());
                DataInputStream rmInStream = new DataInputStream(rm.getInputStream());
                rmStream.writeUTF("GFD:: 0 members");
                String rmSta = rmInStream.readUTF();
                String[] rmStas = rmSta.split("::");
                if (rmStas[1].equals("active")) {
                    dsState = 1;
                } else {
                    dsState = 0;
                }
                System.out.println("GFD connect to RM.");
                isConn = true;
            } catch (IOException e) {
                System.out.println("Can't connect to RM.");
            }
        }
        // GFD is listening on port 5056
        try {
            gfd = new ServerSocket(5050);
            System.out.println("GFD starts with port " + gfd.getLocalPort());
            System.out.println("GFD: " + serverList.size() + " members");
        } catch(IOException e) {
            System.out.println("GFD not listen on port 5050");
            System.exit(-1);
        }
        // running infinite loop for getting
        // client request
        while (true) {
            try {
                // socket object to receive incoming client requests
                ClientHandler cw = new ClientHandler(gfd.accept(), connID++, rmStream);
                Thread t = new Thread(cw);
                t.start();
            } catch (Exception e) {
                gfd.close();
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        GFD.startGFD();
    }

    // ClientHandler class
    private static class ClientHandler extends Thread {
        private Socket client;
        private int lfdId;
        private int lfdServerPort;
        DataOutputStream rmOut;

        // Constructor
        public ClientHandler(Socket client, int lfdID, DataOutputStream rmOut) {
            this.client = client;
            this.lfdId = lfdID;
            this.rmOut = rmOut;
        }

        @Override
        public void run() {
            String line;
            DataInputStream in = null;
            DataOutputStream out = null;
            try {
                in = new DataInputStream(new BufferedInputStream(client.getInputStream()));
                out = new DataOutputStream(client.getOutputStream());
                line = in.readUTF();
                System.out.println(line);
                out.writeUTF("Status " + dsState + " Connection request received: " + this.lfdId);
            } catch (IOException e) {
                e.printStackTrace();
            }
            while (true) {
                try {
                    line = in.readUTF();
                    if (line.contains("add replica")) {
                        System.out.println(line);
                        String serverId = line.split(" ")[line.split(" ").length - 1];
                        synchronized (serverList) {
                            serverList.add(serverId);
                        }
                        System.out.println(Arrays.toString(line.split(" ")));
                        this.lfdServerPort = Integer.parseInt(line.split(" ")[2]);
                        synchronized (rmOut) {
                            rmOut.writeUTF("add::" + serverId + "::" + lfdServerPort);
                        }
                        System.out.println("GFD: " + serverList.size() + " members: " + serverList);
                    } else if (line.contains("delete replica")) {
                        System.out.println(line);
                        String serverId = line.split(" ")[line.split(" ").length - 1];
                        synchronized (serverList) {
                            serverList.remove(serverId);
                        }
                        synchronized (rmOut) {
                            rmOut.writeUTF("delete::" + serverId + "::" + lfdServerPort);
                        }
                        System.out.println("GFD: " + serverList.size() + " members: " + serverList);
                    } else {
                        System.out.println("ldf_" + lfdId + " " + line);
                        long sequenceNumber = Long.parseLong(line.split(" ")[line.split(" ").length - 1]);
                        out.writeUTF("GFD received heartbeat with sequenceNumber " + sequenceNumber);
                    }
                } catch (IOException e) {
                    System.out.println("Lost connection to lfd_" + this.lfdId + " " + this.client);
                    break;
                }
            }
        }
    }
}
