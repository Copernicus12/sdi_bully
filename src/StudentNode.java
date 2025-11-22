import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Schelet pentru nodul student (experiment Bully):
 *
 * - Conexiune la BullyRegistryServer:
 *      * REGISTER <ID> <NODE_PORT>
 *      * raspunde la PING cu PONG
 *      * primeste LIST cu nodurile din retea
 * - Server TCP P2P:
 *      * asculta pe NODE_PORT
 *      * primeste mesaje text: ELECTION, OK, COORDINATOR etc.
 * - Metode TODO pentru logica algoritmului Bully.
 */
public class StudentNode {

    public static class PeerInfo {
        final int id;
        final String ip;
        final int port;

        public PeerInfo(int id, String ip, int port) {
            this.id = id;
            this.ip = ip;
            this.port = port;
        }

        @Override
        public String toString() {
            return id + "@" + ip + ":" + port;
        }
    }

    private final String serverHost;
    private final int serverPort;
    private final int myId;
    private final int listenPort;

    private Socket registrySocket;
    private PrintWriter registryOut;
    private BufferedReader registryIn;

    private final Map<Integer, PeerInfo> peers = new ConcurrentHashMap<>();

    private volatile Integer currentLeader = null;
    private final AtomicBoolean inElection = new AtomicBoolean(false);
    private final AtomicBoolean gotOkInCurrentElection = new AtomicBoolean(false);

    private volatile boolean running = true;
    private ServerSocket p2pServerSocket = null;

    public StudentNode(String serverHost, int serverPort, int myId, int listenPort) {
        this.serverHost = serverHost;
        this.serverPort = serverPort;
        this.myId = myId;
        this.listenPort = listenPort;
    }

    public void start() throws IOException {
        System.out.println("Pornez nod student:");
        System.out.println("  ID          = " + myId);
        System.out.println("  ListenPort  = " + listenPort);
        System.out.println("  Registry    = " + serverHost + ":" + serverPort);

        Thread p2pServerThread = new Thread(this::p2pServerLoop, "P2P-Server");
        p2pServerThread.setDaemon(true);
        p2pServerThread.start();

        registrySocket = new Socket(serverHost, serverPort);
        registryOut = new PrintWriter(new OutputStreamWriter(registrySocket.getOutputStream()), true);
        registryIn  = new BufferedReader(new InputStreamReader(registrySocket.getInputStream()));

        String regCmd = "REGISTER " + myId + " " + listenPort;
        System.out.println("Catre registry: " + regCmd);
        registryOut.println(regCmd);

        Thread registryReaderThread = new Thread(this::registryReadLoop, "Registry-Reader");
        registryReaderThread.setDaemon(true);
        registryReaderThread.start();

        consoleLoop();
    }

    private void shutdownAndExit(int code) {
        running = false;

        System.out.println("Shutting down...");

        try {
            if (registrySocket != null && !registrySocket.isClosed()) {
                registrySocket.close();
            }
        } catch (IOException ignored) {}

        try {
            if (p2pServerSocket != null && !p2pServerSocket.isClosed()) {
                p2pServerSocket.close();
            }
        } catch (IOException ignored) {}

        System.exit(code);
    }

    // ================== SERVER P2P – primire mesaje de la alte noduri ==================

    private void p2pServerLoop() {
        try (ServerSocket serverSocket = new ServerSocket(listenPort)) {
            this.p2pServerSocket = serverSocket;
            System.out.println("P2P server asculta pe portul " + listenPort);

            while (running) {
                Socket s = serverSocket.accept();
                Thread t = new Thread(() -> handlePeerConnection(s), "P2P-ClientHandler");
                t.setDaemon(true);
                t.start();
            }
        } catch (IOException e) {
            System.err.println("Eroare la P2P server: " + e.getMessage());
        }
    }

    private void handlePeerConnection(Socket s) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream()));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream()), true)) {

            String remote = s.getRemoteSocketAddress().toString();
            String line;
            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                System.out.println("[P2P from " + remote + "] " + line);
                handlePeerMessage(line, out);
            }

        } catch (IOException e) {
        }
    }

    private void handlePeerMessage(String line, PrintWriter replyOut) {
        String[] parts = line.split("\\s+");
        String cmd = parts[0].toUpperCase(Locale.ROOT);

        try {
            switch (cmd) {
                case "ELECTION":
                    if (parts.length == 2) {
                        int fromId = Integer.parseInt(parts[1]);
                        onElectionMessage(fromId, replyOut);
                    }
                    break;

                case "OK":
                    if (parts.length == 2) {
                        int fromId = Integer.parseInt(parts[1]);
                        onOkMessage(fromId);
                    }
                    break;

                case "COORDINATOR":
                    if (parts.length == 2) {
                        int leaderId = Integer.parseInt(parts[1]);
                        onCoordinatorMessage(leaderId);
                    }
                    break;

                default:
                    System.out.println("Mesaj P2P necunoscut: " + line);
                    break;
            }
        } catch (NumberFormatException e) {
            System.out.println("Eroare parsare mesaj P2P: " + line + " / " + e);
        }
    }

    // ================== REGISTRY SERVER – citire mesaje ==================

    private void registryReadLoop() {
        String line;
        try {
            while ((line = registryIn.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                if ("PING".equalsIgnoreCase(line)) {
                    System.out.println("[REGISTRY] PING (raspund cu PONG)");
                    registryOut.println("PONG");
                    continue;
                }

                System.out.println("[REGISTRY] " + line);
                handleRegistryMessage(line);
            }
        } catch (IOException e) {
            System.out.println("Conexiunea la registry s-a inchis: " + e.getMessage());
        }
    }

    private void handleRegistryMessage(String line) {

        String[] parts = line.split("\\s+");
        String cmd = parts[0].toUpperCase(Locale.ROOT);

        switch (cmd) {
            case "ACCEPT":
                System.out.println("Inregistrare acceptata pentru ID " + (parts.length >= 2 ? parts[1] : "?"));
                break;

            case "REJECT":
                System.out.println("Inregistrare respinsa: " + line);
                shutdownAndExit(1);
                break;

            case "LIST":
                updatePeersFromList(parts);
                break;

            case "LEADER":
                if (parts.length == 2) {
                    try {
                        int leaderId = Integer.parseInt(parts[1]);
                        System.out.println("Registry anunta liderul = " + leaderId);
                        currentLeader = leaderId;
                    } catch (NumberFormatException ignored) {}
                }
                break;

            default:
                break;
        }
    }

    private void updatePeersFromList(String[] parts) {
        peers.clear();
        for (int i = 1; i < parts.length; i++) {
            String token = parts[i].trim();
            if (token.isEmpty()) continue;

            try {
                String[] idAndRest = token.split("@");
                int id = Integer.parseInt(idAndRest[0]);
                if (id == myId) {
                    continue;
                }

                String[] hostAndPort = idAndRest[1].split(":");
                String ip = hostAndPort[0];
                int port = Integer.parseInt(hostAndPort[1]);

                peers.put(id, new PeerInfo(id, ip, port));
            } catch (Exception e) {
                System.err.println("Eroare parsare LIST: " + token + " / " + e);
            }
        }

        System.out.println("Peers actualizati: " + peers.values());
    }

    // ================== LOGICA BULLY – SCHELET / COMPLETARE ==================


    public void startElection() {
        if (!inElection.compareAndSet(false, true)) {
            System.out.println("Deja sunt in alegeri.");
            return;
        }

        gotOkInCurrentElection.set(false);
        currentLeader = null;

        System.out.println(">>> Pornesc ELECTION. Eu = " + myId);

        boolean sent = false;
        for (PeerInfo p : peers.values()) {
            if (p.id > myId) {
                sendP2PMessage(p, "ELECTION " + myId);
                sent = true;
            }
        }

        if (!sent) {
            becomeLeader();
        } else {

            Thread t = new Thread(() -> {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ignored) {
                }

                if (!inElection.get()) {
                    return;
                }

                if (!gotOkInCurrentElection.get()) {
                    System.out.println("Nu am primit niciun OK, ma autoproclam lider.");
                    becomeLeader();
                    return;
                }

                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ignored) {
                }

                if (!inElection.get()) {
                    return;
                }


                if (currentLeader == null || currentLeader < myId) {
                    System.out.println("Am primit OK dar niciun COORDINATOR, reiau alegerile.");
                    inElection.set(false); // eliberez starea, apoi reiau
                    startElection();
                }
            }, "ElectionTimer-" + myId + "-" + System.currentTimeMillis());
            t.setDaemon(true);
            t.start();
        }
    }

    private void onElectionMessage(int fromId, PrintWriter replyOut) {
        System.out.println("<<< ELECTION de la " + fromId);

        replyOut.println("OK " + myId);


        if (myId > fromId) {
            startElection();
        }
    }

    private void onOkMessage(int fromId) {
        System.out.println("<<< OK de la " + fromId);
        gotOkInCurrentElection.set(true);

    }

    /**
     * Primim COORDINATOR – un nod s-a declarat lider.
     */
    private void onCoordinatorMessage(int leaderId) {
        System.out.println("<<< COORDINATOR: lider = " + leaderId);
        currentLeader = leaderId;
        inElection.set(false);
        gotOkInCurrentElection.set(false);

        // optional: notificam si registry serverul
        registryOut.println("LEADER " + leaderId);
    }

    /**
     * Eu devin lider (cand nu exista nimeni cu ID mai mare sau dupa timeout).
     */
    private void becomeLeader() {
        System.out.println(">>> EU sunt liderul nou! ID = " + myId);
        currentLeader = myId;
        inElection.set(false);

        for (PeerInfo p : peers.values()) {
            sendP2PMessage(p, "COORDINATOR " + myId);
        }

        registryOut.println("LEADER " + myId);
    }

    // ================== TRIMITERE MESAJE P2P ==================

    private void sendP2PMessage(PeerInfo peer, String msg) {
        try (Socket s = new Socket(peer.ip, peer.port);
             PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream()), true)) {

            out.println(msg);
        } catch (IOException e) {
            System.err.println("Nu pot trimite catre " + peer + ": " + e.getMessage());
        }
    }

    // ================== CONSOLE / INPUT USER ==================

    private void consoleLoop() {
        try (BufferedReader consoleIn = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = consoleIn.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                if ("quit".equalsIgnoreCase(line) || "exit".equalsIgnoreCase(line)) {
                    System.out.println("Comanda quit – inchid nodul.");
                    shutdownAndExit(0);
                    break;
                }

                if ("E".equalsIgnoreCase(line)) {
                    System.out.println("Comanda E – pornesc alegeri Bully.");
                    startElection();
                    continue;
                }

                if ("status".equalsIgnoreCase(line)) {
                    System.out.println("Status nod:");
                    System.out.println("  ID          = " + myId);
                    System.out.println("  Leader      = " + currentLeader);
                    System.out.println("  InElection  = " + inElection.get());
                    System.out.println("  Peers       = " + peers.values());
                    continue;
                }

                System.out.println("Comanda necunoscuta: " + line);
                System.out.println("Comenzi: E, status, quit");
            }
        } catch (IOException e) {
            System.err.println("Eroare la citire consola: " + e.getMessage());
        }
    }

    // ================== MAIN ==================

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Utilizare: java StudentNode <serverHost> <serverPort> <ID> <listenPort>");
            System.out.println("Exemplu:  java StudentNode 192.168.0.10 5000 4500 6000");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);
        int id = Integer.parseInt(args[2]);
        int listenPort = Integer.parseInt(args[3]);

        StudentNode node = new StudentNode(host, port, id, listenPort);
        try {
            node.start();
        } catch (IOException e) {
            System.err.println("Eroare la nodul student: " + e.getMessage());
        }
    }
}
