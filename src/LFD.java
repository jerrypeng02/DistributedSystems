import java.io.*;
import java.lang.reflect.Array;
import java.net.*;
import java.util.*;

public class LFD {
    private boolean initialized = false;
    private boolean connected = false;
    private static final int GFD_HEARTBEAT_FREQUENCY = 3000;
    private static final int GFD_HEARTBEAT_TimeOut = 10000;
    private volatile Boolean serverConnected = false;
    private volatile Boolean gfdNoticed = false;
    private volatile Integer lfd_ID = -1;
    private volatile Boolean connectionLost = false;
    private volatile Boolean gfdNoticedLost = false;
    private static volatile Integer dsStatus = -1; // default -1, passive 0, active 1
    private volatile int currServerPort = -1;

    void setLFDId(int id) {
        this.lfd_ID = id;
    }

    int getLFDId() {
        return this.lfd_ID;
    }

    void setServerConnected() {
        this.serverConnected = true;
    }

    void setServerConnectedFalse() {
        this.serverConnected = false;
    }

    boolean getServerConnected() {
        return this.serverConnected;
    }

    void setGFDNoticed() {
        this.gfdNoticed = true;
    }

    void setGfdNoticedFalse() {
        this.gfdNoticed = false;
    }

    boolean getGFDNoticed() {
        return this.gfdNoticed;
    }

    void setConnectionLost() {
        this.connectionLost = true;
    }

    void setConnectionLostFalse() {
        this.connectionLost = false;
    }

    boolean getConnectionLost() {
        return this.connectionLost;
    }

    void setGfdNoticedLost() {
        this.gfdNoticedLost = true;
    }

    void setGfdNoticedLostFalse() {
        this.gfdNoticedLost = false;
    }

    boolean getGfdNoticedLost() {
        return this.gfdNoticedLost;
    }

    void setCurrServerPort(int p) { this.currServerPort = p; }

    int getCurrServerPort() { return this.currServerPort; }

    public static void main(String[] args) throws InterruptedException {
        String gfdAdresss = args[0];
        int gfdPortNum = Integer.parseInt(args[1]);
        int listenPort = Integer.parseInt(args[2]);
        LFD lfd = new LFD();
        List lock = new ArrayList<>();
        HeartBeatWithGFDHandler heartBeatWithGFDHandler = new HeartBeatWithGFDHandler(gfdAdresss, gfdPortNum, GFD_HEARTBEAT_FREQUENCY, GFD_HEARTBEAT_FREQUENCY, lfd, lock);
        Thread heartBeatWithGFDThread = new Thread(heartBeatWithGFDHandler);
        heartBeatWithGFDThread.start();
        HeartBeatWithServerHandler heartBeatWithServerHandler = new HeartBeatWithServerHandler(listenPort, GFD_HEARTBEAT_FREQUENCY, GFD_HEARTBEAT_FREQUENCY, lfd, lock);
        Thread heartBeatWithServerHandlerThread = new Thread(heartBeatWithServerHandler);
        heartBeatWithServerHandlerThread.start();
    }

    private static class HeartBeatWithServerHandler extends Thread {
        private long heartbeatSeq = 1;
        private int port;
        private int freq;
        private int lfd_ID = -1;
        private LFD lfd;
        private List lock;

        public HeartBeatWithServerHandler(int port, int timeout, int freq, LFD lfd, List l) {
            this.port = port;
            this.freq = freq;
            this.lfd = lfd;
            this.lock = l;
            System.out.println("Try connecting to server and listening on port: " + port);
        }

        @Override
        public void run() {
            Socket client  = null;
            String line = null;

            while (true) {

                ServerSocket server;

                while (true) {
                    try {
                        server = new ServerSocket(this.port);
                        client = server.accept();
                        this.lfd.setServerConnected();
                        break;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                try {
                    System.out.println("Writing heartbeats to server: " + client.getLocalAddress());
                    DataInputStream in = new DataInputStream(client.getInputStream());
                    DataOutputStream out = new DataOutputStream(client.getOutputStream());
                    while(true) {
                        if (lfd_ID == -1) lfd_ID = this.lfd.getLFDId();
                        out.writeUTF("LFD_" + lfd_ID + " sequenceNumber " + heartbeatSeq + " dsStatus " + dsStatus);
                        System.out.println("LFD_" + lfd_ID + " heartbeat to Server with heartbeat sequenceNumber" + heartbeatSeq + " dsStatus " + dsStatus);
                        heartbeatSeq++;
                        line = in.readUTF();
                        System.out.println("Server response: " + line);
                        String[] strs = line.split(" ");
                        if (lfd.getCurrServerPort() == -1) {
                            lfd.setCurrServerPort(Integer.parseInt(strs[strs.length - 1]));
                            synchronized (lock) {
                                lock.notify();
                            }
                        }
                        Thread.sleep(this.freq);
                    }
                } catch (Exception e) {
                    this.lfd.setConnectionLost();
                    System.out.println("Lost connection to Server");
                    try {
                        server.close();
                    } catch (IOException ioException) {
                        ioException.printStackTrace();
                    }
                    //return;
                }

            }




        }
    }

    private static class HeartBeatWithGFDHandler extends Thread {
        private long heartbeatSeq = 1;
        private String address;
        private int port;
        private int timeout;
        private int freq;
        private int lfd_ID;
        private LFD lfd;
        private List lock;

        // Constructor
        public HeartBeatWithGFDHandler(String address, int port, int timeout, int freq, LFD lfd, List l) {
            this.address = address;
            this.port = port;
            this.timeout = timeout;
            this.freq = freq;
            this.lfd = lfd;
            this.lock = l;
        }

        @Override
        public void run() {
            try {
                Socket server = new Socket(address, port);
                server.setSoTimeout(timeout);
                System.out.println("Receiving heartbeats from GFD: " + address + " port " + port);
                DataInputStream in = new DataInputStream(server.getInputStream());
                DataOutputStream out = new DataOutputStream(server.getOutputStream());
                out.writeUTF("LFD heartbeat connecton request to GFD");
                String line = in.readUTF();
                dsStatus = Integer.parseInt(line.split(" ")[1]);
                lfd_ID = Integer.parseInt(line.split(" ")[line.split(" ").length - 1]);
                this.lfd.setLFDId(lfd_ID);
                System.out.println("LFD assigned LFD ID by GFD " + lfd_ID);
                while(true) {
                    if (this.lfd.getServerConnected() == true && this.lfd.getGFDNoticed() == false) {
                        synchronized (lock) {
                            while (lfd.getCurrServerPort() == -1) {
                                lock.wait();
                            }
                        }
                        out.writeUTF("LFD" + lfd_ID + " on " + lfd.getCurrServerPort() + " :add replica S" + lfd_ID);
                        System.out.println("LFD" + lfd_ID + " on " + lfd.getCurrServerPort() + " :add replica S" + lfd_ID);
                        this.lfd.setGFDNoticed();

                        this.lfd.setConnectionLostFalse();
                        this.lfd.setGfdNoticedLostFalse();
                    }
                    if (this.lfd.getConnectionLost() == true && this.lfd.getGfdNoticedLost() == false) {
                        out.writeUTF("LFD" + lfd_ID + " on " + lfd.getCurrServerPort() + " :delete replica S" + lfd_ID );
                        this.lfd.setGfdNoticedLost();

                        this.lfd.setGfdNoticedFalse();
                        this.lfd.setServerConnectedFalse();
                    }
                    out.writeUTF("LFD heartbeat " + heartbeatSeq);
                    heartbeatSeq++;
                    line = in.readUTF();
                    System.out.println("GFD address " + address + " port " + port + ": " + line);
                    Thread.sleep(this.freq);
                }
            } catch (Exception e) {
                System.out.println("Lost connection to GFD");
                return;
            }
        }
    }
}
