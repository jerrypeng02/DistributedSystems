// Java implementation of  Server side
// It contains two classes : Server and ClientHandler
// Save file as Server.java

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;


// Server class
public class Server {
    ServerSocket server;
    HashMap<String, Integer> map;
    ReentrantLock lock;
    Set<String> passiveLock;
    Set<String> logs;

    static String serverName;
    static int checkpointNum;
    private static Deque<Integer> serverQueue;
    private static volatile Integer dsStatus = -1; // default -1, passive 0, active 1
    private static int isReady = 0;
    private int messageNum = 0;
    private int targetNum;

    public Server() {
        checkpointNum = 0;
        map = new HashMap<String, Integer>();
        lock = new ReentrantLock();
        passiveLock = new HashSet<>();
        serverName = null;
        serverQueue = new ConcurrentLinkedDeque<>();
        logs = new HashSet<>();
    }

    public void setServerName(String servernName) {
        this.serverName = servernName;
    }

    public void increaseMessageNum() {
        this.messageNum++;
        if (this.messageNum == this.targetNum) {
            synchronized (passiveLock) {
                passiveLock.notify();
            }
            this.messageNum = 0;
        }
    }

    public void setTargetNum(int val) { this.targetNum = val; }

    String getServerName() {
        return this.serverName;
    }

    public static void startServer(String[] args) throws IOException {
        int serverPortNum = Integer.parseInt(args[0]);
        String lfdAdresss= args[1];
        int lfdPortNumber = Integer.parseInt(args[2]);
        Server serverWrapper = new Server();
        int checkpointFreq = Integer.parseInt(args[3]);
        serverWrapper.setTargetNum(checkpointFreq);

        try {

            Socket lfdSocket = new Socket(lfdAdresss, lfdPortNumber);
            DataInputStream htbtLfdIn = new DataInputStream(lfdSocket.getInputStream());
            DataOutputStream htbtLfdOut = new DataOutputStream(lfdSocket.getOutputStream());
            String line = htbtLfdIn.readUTF();
            System.out.println(line + " from LFD address " + lfdAdresss + " port " + lfdPortNumber);
            String[] lineStrings = line.split(" ");
            String lfdId = lineStrings[0];
            dsStatus = Integer.parseInt(lineStrings[4]);
            serverWrapper.setServerName("Server_" + lfdId.split("_")[1]);
            long sequenceNumber = Long.parseLong(lineStrings[2]);
            htbtLfdOut.writeUTF(serverWrapper.getServerName() + " receive heartbeats " + sequenceNumber + " address " + serverPortNum);

            HeartBeatWithLFDHandler heartBeatWithLFDHandler = new HeartBeatWithLFDHandler(lfdAdresss, lfdPortNumber, serverWrapper, htbtLfdIn, htbtLfdOut);
            Thread heartBeatWithLFDThread = new Thread(heartBeatWithLFDHandler);
            heartBeatWithLFDThread.start();
            if (dsStatus == -1) {
                System.out.println("Mode unset.");
                return;
            }

            serverWrapper.server = new ServerSocket(serverPortNum);
            System.out.println("Server starts with port " + serverPortNum);
            Set<Deque<String>> inputQueue = new HashSet<>();

            MakeConnectionThread mkConn = new MakeConnectionThread(serverQueue, inputQueue);
            Thread mkConnThread = new Thread(mkConn);
            mkConnThread.start();

            CheckpointThread ckpt = new CheckpointThread(checkpointFreq, inputQueue, serverWrapper);
            Thread ckptThread = new Thread(ckpt);
            ckptThread.start();


        } catch(IOException e) {
            System.out.println("Could not listen on port");
            System.exit(-1);
        }

        while (true) {
            try {
                ClientHandler cw = new ClientHandler(serverWrapper.server.accept(), serverWrapper, serverQueue);
                Thread t = new Thread(cw);
                t.start();

            } catch (Exception e) {
                serverWrapper.server.close();
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        int serverPortNum = Integer.parseInt(args[0]);
        String lfdAdresss= args[1];
        int lfdPortNumber = Integer.parseInt(args[2]);
        Server serverWrapper = new Server();
        int checkpointFreq = Integer.parseInt(args[3]);
        serverWrapper.setTargetNum(checkpointFreq);

        try {

            Socket lfdSocket = new Socket(lfdAdresss, lfdPortNumber);
            DataInputStream htbtLfdIn = new DataInputStream(lfdSocket.getInputStream());
            DataOutputStream htbtLfdOut = new DataOutputStream(lfdSocket.getOutputStream());
            String line = htbtLfdIn.readUTF();
            System.out.println(line + " from LFD address " + lfdAdresss + " port " + lfdPortNumber);
            String[] lineStrings = line.split(" ");
            String lfdId = lineStrings[0];
            dsStatus = Integer.parseInt(lineStrings[4]);
            serverWrapper.setServerName("Server_" + lfdId.split("_")[1]);
            long sequenceNumber = Long.parseLong(lineStrings[2]);
            htbtLfdOut.writeUTF(serverWrapper.getServerName() + " receive heartbeats " + sequenceNumber + " address " + serverPortNum);

            HeartBeatWithLFDHandler heartBeatWithLFDHandler = new HeartBeatWithLFDHandler(lfdAdresss, lfdPortNumber, serverWrapper, htbtLfdIn, htbtLfdOut);
            Thread heartBeatWithLFDThread = new Thread(heartBeatWithLFDHandler);
            heartBeatWithLFDThread.start();
            if (dsStatus == -1) {
                System.out.println("Mode unset.");
                return;
            }

            serverWrapper.server = new ServerSocket(serverPortNum);
            System.out.println("Server starts with port " + serverPortNum);
            Set<Deque<String>> inputQueue = new HashSet<>();

            MakeConnectionThread mkConn = new MakeConnectionThread(serverQueue, inputQueue);
            Thread mkConnThread = new Thread(mkConn);
            mkConnThread.start();

            CheckpointThread ckpt = new CheckpointThread(checkpointFreq, inputQueue, serverWrapper);
            Thread ckptThread = new Thread(ckpt);
            ckptThread.start();


        } catch(IOException e) {
            System.out.println("Could not listen on port");
            System.exit(-1);
        }

        while (true) {
            try {
                ClientHandler cw = new ClientHandler(serverWrapper.server.accept(), serverWrapper, serverQueue);
                Thread t = new Thread(cw);
                t.start();

            } catch (Exception e) {
                serverWrapper.server.close();
                e.printStackTrace();
            }
        }
    }

    private static class HeartBeatWithLFDHandler extends Thread {
        private String address;
        private int port;
        private Server serverWrapper;
        private DataInputStream in;
        private DataOutputStream out;

        public HeartBeatWithLFDHandler(String address, int port, Server serverWrapper, DataInputStream in, DataOutputStream out) {
            this.address = address;
            this.port = port;
            this.serverWrapper = serverWrapper;
            this.in = in;
            this.out = out;
        }

        @Override
        public void run() {
            String line;
            try {
                System.out.println("Receiving heartbeats from LFD: " + address + " port " + port);
                while(true) {
                    line = in.readUTF();
                    System.out.println(line + " from LFD address " + address + " port " + port);
                    String[] lineStrings = line.split(" ");
                    long sequenceNumber = Long.parseLong(lineStrings[2]);
                    out.writeUTF(this.serverWrapper.getServerName() + " receive heartbeats " + sequenceNumber + " address " + this.serverWrapper.server.getLocalPort());
                }
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Lost connection to LFD");
                return;
            }
        }
    }


    // ClientHandler class
    static class ClientHandler extends Thread {
        private Socket client;
        private Server serverWrapper;
        private Deque<Integer> serverQue;

        // Constructor
        public ClientHandler(Socket client, Server serverWrapper, Deque<Integer> q) {
            this.client = client;
            this.serverWrapper = serverWrapper;
            this.serverQue = q;
        }

        @Override
        public void run() {
            String line;
            DataInputStream in = null;
            DataOutputStream out = null;
            try {
                in = new DataInputStream(new BufferedInputStream(client.getInputStream()));
                out = new DataOutputStream(client.getOutputStream());
            } catch (IOException e) {
                e.printStackTrace();
            }
            String currConnect = null;
            while (true) {
                try {
                    line = in.readUTF();
                    String[] lines = line.split("::");
                    currConnect = lines[0];
                    if (lines[0].equals("C")) {
                        if ((dsStatus == 1 && isReady == 1) || dsStatus == 0) {
                            clientRun(lines[1], out);
                        } else if (!lines[1].equals("Connection request")) {
                            String message = lines[1].split("\\|")[1];
                            this.serverWrapper.lock.lock();
                            this.serverWrapper.map.put(message , this.serverWrapper.map.getOrDefault(message , 0) + 1);
                            this.serverWrapper.lock.unlock();
                        }
                    } else if (lines[0].equals("S")){
                        serverRun(lines[1]);
                    } else {
                        replicaManagerRun(lines[1]);
                    }
                } catch (IOException | InterruptedException e) {
                    if (currConnect == null || (currConnect.equals("S") && dsStatus == 0)) {
                        System.out.println("Lost connection to " + this.client);
                    }
                    break;
                }
            }
        }

        private void clientRun(String line, DataOutputStream out) throws IOException {
            if (!line.equals("Connection request")) {
                if (isReady == 1) {
                    String message = line.split("\\|")[1];
                    System.out.println(this.serverWrapper.getServerName() + " received message: " + line);
                    this.serverWrapper.lock.lock();
                    this.serverWrapper.map.put(message , this.serverWrapper.map.getOrDefault(message , 0) + 1);
                    this.serverWrapper.increaseMessageNum();
                    this.serverWrapper.lock.unlock();
                    System.out.println("my state: " + this.serverWrapper.map);
                    System.out.println(this.client + line);
                    out.writeUTF(this.serverWrapper.getServerName() + ": " + line);
                } else {
                    synchronized (serverWrapper.logs) {
                        serverWrapper.logs.add(line);
                    }
                }

            } else {
                System.out.println(this.client + line);
                out.writeUTF(this.serverWrapper.getServerName() + " Connected. Status: " + isReady);
            }
        }

        private void serverRun(String line) throws InterruptedException {
            if(!line.equals("Connection request")) {
                String[] messages = line.split(", ");
                int passInCkptNum = Integer.parseInt(messages[0]) + 1;
                if (checkpointNum < passInCkptNum) {
                    checkpointNum = passInCkptNum;
                    System.out.println("Checkpoint Num: " + checkpointNum);
                    this.serverWrapper.lock.lock();
                    serverWrapper.map.clear();

                    for (int i = 1; i < messages.length; i++) {
                        String curr = messages[i];
                        String[] mapData = curr.split("=");
                        if (dsStatus == 0) {
                            serverWrapper.map.put(mapData[0], Integer.parseInt(mapData[1]));
                        } else {
                            serverWrapper.map.put(mapData[0], serverWrapper.map.getOrDefault(mapData[0], 0) + Integer.parseInt(mapData[1]));
                        }
                    }
                    Thread.sleep(300); // make sure executed the client message.
                    synchronized (serverWrapper.logs) {
                        serverWrapper.logs.clear();
                    }

                    this.serverWrapper.lock.unlock();
                    System.out.println("Update State: " + serverWrapper.map.toString());
                    synchronized (dsStatus) {
                        if (dsStatus == 1) {
                            isReady = 1;
                        }
                    }
                } else {
                    System.out.println("Drop duplicate checkpoint");
                }
            }
        }

        private void replicaManagerRun(String line) {
            if(!line.equals("Connection request")) {
                String[] messages = line.split(" ");
                if (Integer.parseInt(messages[0]) == 1 || dsStatus == 0) {
                    isReady = 1;
                    synchronized (serverWrapper.logs) {
                        if (!serverWrapper.logs.isEmpty()) {
                            for (String s : serverWrapper.logs) {
                                String message = s.split("\\|")[1];
                                this.serverWrapper.lock.lock();
                                this.serverWrapper.map.put(message , this.serverWrapper.map.getOrDefault(message , 0) + 1);
                                this.serverWrapper.increaseMessageNum();
                                this.serverWrapper.lock.unlock();
                            }
                            serverWrapper.logs.clear();
                        }
                    }
                }
                synchronized (serverQue) {
                    for (int i = 1; i < messages.length; i++) {
                        int newPortNum = Integer.parseInt(messages[i]);
                        serverQue.offerLast(newPortNum);
                    }
                    serverQue.notify();
                }
            }
        }
    }

    static class ServerHandler extends Thread {
        String address;
        int port;
        Deque<String> q;
        Set<Deque<String>> inputSet;

        public ServerHandler(String address, int port, Deque<String> tempQ, Set<Deque<String>> inSet) {
            this.address = address;
            this.port = port;
            //this.checkpointFreq = checkpointFreq;
            this.q = tempQ;
            inputSet = inSet;
        }

        @Override
        public void run() {
            Socket socket = null;
            DataOutputStream out = null;
            boolean isConn = false;
            while (!isConn) {
                try {
                    socket = new Socket(address, port);
                    out = new DataOutputStream(socket.getOutputStream());
                    isConn = true;
                } catch (IOException e) {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException interruptedException) {
                        interruptedException.printStackTrace();
                    }
                }
            }
            try {
                while (true) {
                    synchronized (q) {
                        while (q.isEmpty()) {
                            try {
                                q.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        if (!q.isEmpty()) {
                            String state = q.pollFirst();
                            out.writeUTF(state);
                        }
                    }
                    if (dsStatus == 1) break;
                }
            } catch (IOException e) {
                System.out.println("Lost Connection to " + socket);
            }
            synchronized (inputSet) {
                inputSet.remove(q);
            }
        }
    }

    static class CheckpointThread extends Thread {
        private int checkpointFreq;
        private Set<Deque<String>> inputSet;
        private Server serverWrapper;

        public CheckpointThread(int freq, Set<Deque<String>> inputS, Server serv) {
            checkpointFreq = freq;
            inputSet = inputS;
            serverWrapper = serv;
        }

        @Override
        public void run() {
            try {
                while (true) {


                    String state = serverWrapper.map.toString();
                    state = state.replaceAll("\\{", "").replaceAll("\\}", "");
                    state = "S::" + checkpointNum + ", " + state;
                    boolean checked = false;
                    if (!inputSet.isEmpty()) checked = true;
                    synchronized (inputSet) {
                        for (Deque<String> q: inputSet) {
                            synchronized (q) {
                                q.clear();
                                q.addLast(state);
                                q.notify();
                            }
                        }
                    }
                    if (checked) {
                        checkpointNum++;
                        System.out.println("Checkpoint Num: " + checkpointNum + " " + serverWrapper.map.toString());
                    }

                    if (dsStatus == 0) {
                        synchronized (serverWrapper.passiveLock) {
                            serverWrapper.passiveLock.wait();
                        }
                    }


                    if (dsStatus == 1) {
                        synchronized (inputSet) {
                            inputSet.wait();
                        }
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }

    static class MakeConnectionThread extends Thread {
        private Deque<Integer> serverDeque;
        private Set<Deque<String>> inputSet;

        public MakeConnectionThread(Deque<Integer> serverQ, Set<Deque<String>> inputS) {
            serverDeque = serverQ;
            inputSet = inputS;
        }

        @Override
        public void run() {
            while (true) {
                synchronized (serverDeque) {
                    while (serverDeque.isEmpty()) {
                        try {
                            serverDeque.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
                synchronized (inputSet) {
                    synchronized (serverDeque) {
                        while (!serverDeque.isEmpty()) {
                            Deque<String> currQ = new ConcurrentLinkedDeque<>();
                            inputSet.add(currQ);
                            ServerHandler backupThread = new ServerHandler("127.0.0.1", serverDeque.pollFirst(), currQ, inputSet);
                            Thread t = new Thread(backupThread);
                            t.start();
                        }
                        inputSet.notify();
                    }
                }
            }
        }
    }

}

