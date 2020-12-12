import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.Process;
import java.lang.ProcessBuilder;
import java.lang.reflect.Field;


public class RM {
    public static boolean isActive = false;
    public static List<ServerInfo> membership = new ArrayList<>();
    public static int mode = 0;
    public static int autoRecovery = 0;
    public static ServerInfo primaryServer = null;
    public static final String[] GFD_CONFIGURATION = new String[] {"java", "GFD"};
    public static final String[] LFD1_CONFIGURATION = new String[] {"java", "LFD", "127.0.0.1", "5050", "4000"};
    public static final String[] LFD2_CONFIGURATION = new String[] {"java", "LFD", "127.0.0.1", "5050", "4002"};
    public static final String[] LFD3_CONFIGURATION = new String[] {"java", "LFD", "127.0.0.1", "5050", "4004"};
    public static final String[][] Server_CONFIGURATION = new String[][] { {"java", "Server", "5056", "127.0.0.1", "4000", "3"},
            {"java", "Server", "5058", "127.0.0.1", "4002", "3"},
            {"java", "Server", "5060", "127.0.0.1", "4004", "3"},
    };


    public static synchronized long getPidOfProcess(Process p) {
        long pid = -1;
        try {
            Field f = p.getClass().getDeclaredField("pid");
            f.setAccessible(true);
            pid = f.getLong(p);
            f.setAccessible(false);
        } catch (Exception e) {
            e.printStackTrace();
            pid = -1;
        }
        return pid;
    }

    public static void startSystem() {
        try {
            Thread.sleep(1000);
            ProcessBuilder pb = new ProcessBuilder(GFD_CONFIGURATION);
            pb.inheritIO();
            Process process = pb.start();
            long pid = RM.getPidOfProcess(process);
            System.out.printf("GFD pid = %d\n", pid);

            Thread.sleep(500);
            pb = new ProcessBuilder(LFD1_CONFIGURATION);
            pb.inheritIO();
            process = pb.start();
            pid = RM.getPidOfProcess(process);
            System.out.printf("LFD1 pid = %d\n", pid);


            Thread.sleep(500);
            pb = new ProcessBuilder(LFD2_CONFIGURATION);
            pb.inheritIO();
            process = pb.start();
            pid = RM.getPidOfProcess(process);
            System.out.printf("LFD2 pid = %d\n", pid);

            Thread.sleep(500);
            pb = new ProcessBuilder(LFD3_CONFIGURATION);
            pb.inheritIO();
            process = pb.start();
            pid = RM.getPidOfProcess(process);
            System.out.printf("LFD3 pid = %d\n", pid);
            Thread.sleep(2000);

            pb = new ProcessBuilder(Server_CONFIGURATION[0]);
            pb.inheritIO();
            process = pb.start();
            pid = RM.getPidOfProcess(process);
            System.out.printf("Server1 pid = %d\n", pid);
            Thread.sleep(1000);

            pb = new ProcessBuilder(Server_CONFIGURATION[1]);
            pb.inheritIO();
            process = pb.start();
            pid = RM.getPidOfProcess(process);
            System.out.printf("Server2 pid = %d\n", pid);
            Thread.sleep(1000);

            pb = new ProcessBuilder(Server_CONFIGURATION[2]);
            pb.inheritIO();
            process = pb.start();
            pid = RM.getPidOfProcess(process);
            System.out.printf("Server3 pid = %d\n", pid);
            Thread.sleep(1000);

        } catch (Exception e) {
            System.out.println("Start GFD, LFD, Server Failed");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println("Replica Manager started");
        System.err.close();
        System.setErr(System.out);
        int serverPortNum = Integer.parseInt(args[0]);
        mode = Integer.parseInt(args[1]);
        autoRecovery = Integer.parseInt(args[2]);
        if(mode == 1) {
            System.out.println("Choosing active mode");
            isActive = true;
        } else {
            System.out.println("Choosing passive mode");
        }
        if (autoRecovery == 1) {
            System.out.println("Choosing auto recovery mode");
        } else {
            System.out.println("Choosing manual recovery mode");
        }
        ServerSocket socket = null;
        try {
            socket = new ServerSocket(serverPortNum);
        } catch (IOException e) {
            System.out.println("Failed to initialize Replica Manager");
        }
        StartSystemThread startSystemThread = new StartSystemThread();
        Thread t1 = new Thread(startSystemThread);
        t1.start();
        while(true) {
            try {
                GFDHandler handler = new GFDHandler(socket.accept());
                Thread t2 = new Thread(handler);
                t2.start();
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
                        if (autoRecovery == 1) {
                            System.out.printf("System tries to restart server %d in auto recovery mode\n", port);
                            for (String [] configuration : Server_CONFIGURATION) {
                                if (Integer.valueOf(configuration[2]).equals(port)) {
                                    AutoRecovery autoRecovery = new AutoRecovery(configuration);
                                    Thread t1 = new Thread(autoRecovery);
                                    t1.start();
                                }
                            }
                        }
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

    public static class AutoRecovery extends Thread {
        String[] configuration;
        public AutoRecovery(String[] configuration) {
            this.configuration = configuration;
        }
        @Override
        public void run() {
            try {
                Thread.sleep(1000);
                ProcessBuilder pb = new ProcessBuilder(this.configuration);
                pb.inheritIO();
                Process process = pb.start();
                long pid = RM.getPidOfProcess(process);
                System.out.printf("Recoverd Server %s pid = %d\n", this.configuration[2], pid);

            } catch (Exception e) {
                System.out.printf("Recover server %s fialed \n", this.configuration[2]);
                e.printStackTrace();
            }
        }
    }

    public static class StartSystemThread extends Thread {
        public StartSystemThread() {
        }
        @Override
        public void run() {
            RM.startSystem();
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
