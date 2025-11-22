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

    // info despre un alt nod din retea
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

    // lista de noduri cunoscute (fara mine) – id -> PeerInfo
    private final Map<Integer, PeerInfo> peers = new ConcurrentHashMap<>();

    // stare pentru Bully (foarte simplificata)
    private volatile Integer currentLeader = null;
    private final AtomicBoolean inElection = new AtomicBoolean(false);
    private final AtomicBoolean gotOkInCurrentElection = new AtomicBoolean(false);

    // variabile de instanta pentru rulare/stop
    private volatile boolean running = true;
    private ServerSocket p2pServerSocket = null; // daca e accesibil

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

        // 1. Pornim serverul P2P local (asculta mesaje de la alte noduri)
        Thread p2pServerThread = new Thread(this::p2pServerLoop, "P2P-Server");
        p2pServerThread.setDaemon(true);
        p2pServerThread.start();

        // 2. Conectare la BullyRegistryServer
        registrySocket = new Socket(serverHost, serverPort);
        registryOut = new PrintWriter(new OutputStreamWriter(registrySocket.getOutputStream()), true);
        registryIn  = new BufferedReader(new InputStreamReader(registrySocket.getInputStream()));

        // Trimitem REGISTER
        String regCmd = "REGISTER " + myId + " " + listenPort;
        System.out.println("Catre registry: " + regCmd);
        registryOut.println(regCmd);

        // 3. Thread pentru citirea mesajelor de la registry
        Thread registryReaderThread = new Thread(this::registryReadLoop, "Registry-Reader");
        registryReaderThread.setDaemon(true);
        registryReaderThread.start();

        // 4. Thread consola (citire comenzi de la tastatura)
        consoleLoop();
    }

    // apelata cand vrem sa inchidem curat
    private void shutdownAndExit(int code) {
        running = false;

        System.out.println("Shutting down...");

        // inchidem socketul catre registry
        try {
            if (registrySocket != null && !registrySocket.isClosed()) {
                registrySocket.close();
            }
        } catch (IOException ignored) {}

        // inchidem server socket P2P (daca exista)
        try {
            if (p2pServerSocket != null && !p2pServerSocket.isClosed()) {
                p2pServerSocket.close();
            }
        } catch (IOException ignored) {}

        // optional: asteptam thread-urile sa se termine (join) — daca le pastrezi ca referinta

        // iesim din JVM cu cod de eroare
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
            // conexiune P2P inchisa
        }
    }

    // Mesaje P2P propuse (schelet):
    // ELECTION <fromId>
    // OK <fromId>
    // COORDINATOR <leaderId>
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
        // Posibile mesaje:
        // ACCEPT <ID>
        // REJECT ...
        // LIST <id1>@<ip1>:<port1> <id2>@<ip2>:<port2> ...
        // LEADER <id_lider>
        String[] parts = line.split("\\s+");
        String cmd = parts[0].toUpperCase(Locale.ROOT);

        switch (cmd) {
            case "ACCEPT":
                System.out.println("Inregistrare acceptata pentru ID " + (parts.length >= 2 ? parts[1] : "?"));
                break;

            case "REJECT":
                System.out.println("Inregistrare respinsa: " + line);
                // clean shutdown
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
                // INFO, alte mesaje – doar afisam
                break;
        }
    }

    // LIST <id1>@<ip1>:<port1> <id2>@<ip2>:<port2> ...
    private void updatePeersFromList(String[] parts) {
        peers.clear();
        for (int i = 1; i < parts.length; i++) {
            String token = parts[i].trim();
            if (token.isEmpty()) continue;

            // format: id@ip:port
            try {
                String[] idAndRest = token.split("@");
                int id = Integer.parseInt(idAndRest[0]);
                if (id == myId) {
                    continue; // nu ma adaug pe mine
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

    /**
     * Porneste o procedura de alegere Bully.
     */
    public void startElection() {
        // Daca deja sunt in alegeri, nu mai pornesc alta procedura
        if (!inElection.compareAndSet(false, true)) {
            System.out.println("Deja sunt in alegeri.");
            return;
        }

        // resetam starea specifica unei alegeri
        gotOkInCurrentElection.set(false);
        currentLeader = null;

        System.out.println(">>> Pornesc ELECTION. Eu = " + myId);

        // Trimitem ELECTION tuturor nodurilor cu ID mai mare ca al meu
        boolean sent = false;
        for (PeerInfo p : peers.values()) {
            if (p.id > myId) {
                sendP2PMessage(p, "ELECTION " + myId);
                sent = true;
            }
        }

        if (!sent) {
            // Nu exista nimeni cu ID mai mare => devin direct lider (bully clasic)
            becomeLeader();
        } else {
            // Asteptam un timp raspunsuri OK / COORDINATOR etc.
            // Folosim un thread separat pentru a nu bloca firul principal.
            Thread t = new Thread(() -> {
                try {
                    // 1) asteptam o perioada ca sa vedem daca primim vreun OK
                    Thread.sleep(3000);
                } catch (InterruptedException ignored) {
                }

                // Daca intre timp s-a incheiat alegerea, nu mai facem nimic
                if (!inElection.get()) {
                    return;
                }

                if (!gotOkInCurrentElection.get()) {
                    // Nu am primit niciun OK -> inseamna ca nu exista nod mai puternic activ
                    System.out.println("Nu am primit niciun OK, ma autoproclam lider.");
                    becomeLeader();
                    return;
                }

                // Daca am primit OK, inseamna ca exista un nod mai puternic
                // Asteptam COORDINATOR de la acesta.
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException ignored) {
                }

                if (!inElection.get()) {
                    return;
                }

                // Daca nici acum nu am primit COORDINATOR (currentLeader inca null
                // sau are ID mai mic decat al meu), pornesc o noua alegere.
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

    /**
     * Primim un mesaj ELECTION de la alt nod.
     * Implementare Bully clasica:
     *  - raspundem cu OK (daca suntem activi)
     *  - daca ID-ul meu e mai mare decat al emitentului si nu sunt deja in alegeri,
     *    pornesc o alegere noua.
     */
    private void onElectionMessage(int fromId, PrintWriter replyOut) {
        System.out.println("<<< ELECTION de la " + fromId);

        // 1) Trimit inapoi OK (confirm ca exist)
        replyOut.println("OK " + myId);

        // 2) Daca ID-ul meu e mai mare, pornesc propria alegere
        if (myId > fromId) {
            startElection();
        }
        // daca ID-ul meu e mai mic sau egal, doar trimit OK si nu mai fac nimic
    }

    /**
     * Primim OK de la un nod cu ID mai mare (in principiu).
     * Folosit pentru a sti ca exista un "nod mai puternic".
     */
    private void onOkMessage(int fromId) {
        System.out.println("<<< OK de la " + fromId);
        // Marcam faptul ca exista un nod mai puternic activ.
        gotOkInCurrentElection.set(true);
        // In aceasta implementare nu facem altceva aici:
        // firul de timp din startElection() se va ocupa sa astepte
        // COORDINATOR sau sa reia alegerile la nevoie.
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

        // Anuntam toate celelalte noduri P2P
        for (PeerInfo p : peers.values()) {
            sendP2PMessage(p, "COORDINATOR " + myId);
        }

        // Optional: anuntam si registry serverul
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
