import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RM {
    public static boolean isActive = false;
    public static List<ServerInfo> membership = new ArrayList<>();
    public static int mode = 0;
    public static ServerInfo primaryServer = null;

    public static void main(String[] args) throws IOException {
        System.out.println("Replica Manager started");
        int serverPortNum = Integer.parseInt(args[0]);
        mode = Integer.parseInt(args[1]);
        if(mode == 1) {
            System.out.println("Choosing active mode");
            isActive = true;
        } else {
            System.out.println("Choosing passive mode");
        }
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(serverPortNum);
        } catch (IOException e) {
            System.out.println("Failed to initialize Replica Manager");
        }

        while(true) {
            try {
                GFDHandler handler = new GFDHandler(socket.accept());
                Thread t = new Thread(handler);
                t.start();
            } catch (IOException e) {
                socket.close();
            }
        }
    }

    public static class GFDHandler extends Thread {
        private final Socket gfd;

        public GFDHandler(Socket gfd) {
            this.gfd = gfd;
        }

        @Override
        public void run() {
            String line;
            DataInputStream in;
            DataOutputStream out;
            try {
                in = new DataInputStream(new BufferedInputStream(gfd.getInputStream()));
                out = new DataOutputStream(gfd.getOutputStream());
            } catch (IOException e) {
                System.out.println("Lost connection to GFD from RM");
                return;
            }
            while(true) {
                try {
                    if(mode == 1) {
                        out.writeUTF("mode::active");
                    } else {
                        out.writeUTF("mode::passive");
                    }

                    line = in.readUTF();
                    String[] lines = line.split("::");
                    int serverID = -1;
                    int port = -1;
                    if (lines[0].equals("add") || lines[0].equals("delete")) {
                        serverID = Integer.parseInt(lines[1].split("S")[1]);
                        port = Integer.parseInt(lines[2]);
                    }
                    ServerHandler server;
                    ServerInfo si;
                    if (lines[0].equals("add")) {
                        if(mode == 1) {
                            if(membership.size() == 0) {
                                si = new ServerInfo(serverID, 1, port, false);
                                String message = "RM::1";
                                server = new ServerHandler("127.0.0.1", port, message);
                                Thread t = new Thread(server);
                                t.start();
                            } else {
                                for (ServerInfo member : membership) {
                                    String message = "RM::0 " + port;
                                    server = new ServerHandler("127.0.0.1", member.port, message);
                                    Thread t = new Thread(server);
                                    t.start();
                                }
                                si = new ServerInfo(serverID, 1, port, false);
                            }
                        } else {
                            if(membership.size() == 0) {
                                si = new ServerInfo(serverID, 1, port, true);
                                primaryServer = si;
                                String message = "RM::1";
                                server = new ServerHandler("127.0.0.1", port, message);
                                Thread t = new Thread(server);
                                t.start();
                            } else {
                                String message = "RM::0 " + port;
                                server = new ServerHandler("127.0.0.1", primaryServer.port, message);
                                Thread t = new Thread(server);
                                t.start();
                                si = new ServerInfo(serverID, 1, port, false);
                            }
                        }
                        membership.add(si);
                        StringBuilder currMember = new StringBuilder();
                        for (ServerInfo serverInfo : membership) {
                            currMember.append(" S").append(serverInfo.serverID);
                        }
                        System.out.println("RM: " + membership.size() + " member:" + currMember);
                    } else if (lines[0].equals("delete")) {
                        if(mode == 1) {
                            for(int i = 0; i < membership.size(); i++) {
                                ServerInfo member = membership.get(i);
                                if(member.port == port) {
                                    membership.remove(member);
                                    break;
                                }
                            }
                        } else {
                            //System.out.println("delete " +  primaryServer.port);
                            if(primaryServer.port == port) {
                                for(int i = 0; i < membership.size(); i++) {
                                    ServerInfo member = membership.get(i);
                                    if (member.port == port) {
                                        membership.remove(member);
                                        break;
                                    }
                                }
                                for(int i = 0; i < membership.size(); i++) {
                                    ServerInfo member = membership.get(i);
                                    if(member.readyState == 1) {
                                        primaryServer = member;
                                        StringBuilder message = new StringBuilder("RM::0");
                                        for (ServerInfo mem : membership) {
                                            if (mem != primaryServer) {
                                                message.append(" ").append(mem.port);
                                            }
                                        }
                                        server = new ServerHandler("127.0.0.1", primaryServer.port, message.toString());
                                        Thread t = new Thread(server);
                                        t.start();
                                        break;
                                    }
                                }
                            } else {
                                for(int i = 0; i < membership.size(); i++) {
                                    ServerInfo member = membership.get(i);
                                    if(member.port == port) {
                                        membership.remove(member);
                                        break;
                                    }
                                }
                            }
                        }
                        StringBuilder currMember = new StringBuilder();
                        for (ServerInfo serverInfo : membership) {
                            currMember.append(" S").append(serverInfo.serverID);
                        }
                        System.out.println("RM: " + membership.size() + " member:" + currMember);
                    } else {
                        System.out.println("RM: 0 members");
                    }
                } catch (IOException e) {
                    System.out.println("Lost connection to GFD from RM");
                    break;
                }
            }
        }
    }

    public static class ServerHandler extends Thread {
        private final String address;
        private final int port;
        private final String message;

        public ServerHandler(String address, int port, String message) {
            this.address = address;
            this.port = port;
            this.message = message;
        }

        @Override
        public void run() {
            Socket socket;
            DataOutputStream out;
            boolean connectedAndSent = false;
            while(!connectedAndSent) {
                try {
                    socket = new Socket(address, port);
                    out = new DataOutputStream(socket.getOutputStream());
                    out.writeUTF(this.message);
                    connectedAndSent = true;
                    socket.close();
                } catch (IOException i) {
                    System.out.println("Failed to connect server from RM " + port);
                }
            }
        }
    }

    public static class ServerInfo {
        int serverID;
        int readyState;
        int port;
        boolean isPrimary;

        public ServerInfo(int id, int state, int port, boolean isPrimary) {
            this.serverID = id;
            this.readyState = state;
            this.port = port;
            this.isPrimary = isPrimary;
        }
    }
}
