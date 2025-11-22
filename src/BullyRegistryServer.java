import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BullyRegistryServer pentru laborator — compatibil cu StudentNode.
 *
 * Funcționalități:
 *  - Acceptă conexiuni de la noduri StudentNode
 *  - PRIMEȘTE:
 *      REGISTER <id> <port>
 *      PONG
 *      LEADER <id>
 *
 *  - TRIMITE:
 *      ACCEPT <id>
 *      REJECT <motiv>
 *      PING (periodic)
 *      LIST id@ip:port ...
 *      LEADER <id>
 *
 *  - Monitorizează dacă nodurile răspund la PING
 *  - Elimină nodurile moarte
 *  - Trimite LIST actualizat la toate nodurile active
 */

public class BullyRegistryServer {

    static class NodeInfo {
        int id;
        String ip;
        int port;
        Socket socket;
        PrintWriter out;
        volatile boolean alive = true;
        volatile long lastPong = System.currentTimeMillis();

        NodeInfo(int id, String ip, int port, Socket socket, PrintWriter out) {
            this.id = id;
            this.ip = ip;
            this.port = port;
            this.socket = socket;
            this.out = out;
        }

        @Override
        public String toString() {
            return id + "@" + ip + ":" + port;
        }
    }

    private final int listenPort;
    private final Map<Integer, NodeInfo> nodes = new ConcurrentHashMap<>();
    private volatile Integer currentLeader = null;

    public BullyRegistryServer(int port) {
        this.listenPort = port;
    }

    public void start() throws IOException {
        ServerSocket serverSocket = new ServerSocket(listenPort);
        System.out.println("=== BullyRegistryServer pornit pe port " + listenPort + " ===");

        // thread pentru PING periodic
        Thread pingThread = new Thread(this::pingLoop, "PING-Thread");
        pingThread.setDaemon(true);
        pingThread.start();

        // loop pentru conexiuni noi
        while (true) {
            Socket s = serverSocket.accept();
            Thread t = new Thread(() -> handleConnection(s), "ClientHandler");
            t.setDaemon(true);
            t.start();
        }
    }

    private void handleConnection(Socket socket) {
        String remote = socket.getRemoteSocketAddress().toString();
        System.out.println("Conexiune nouă: " + remote);

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()), true);

            String line;

            while ((line = in.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                System.out.println("[" + remote + "] " + line);
                handleMessage(line, socket, out);
            }

        } catch (Exception e) {
            System.out.println("Conexiune pierdută cu: " + remote);
        } finally {
            removeSocket(socket);
        }
    }

    private void handleMessage(String line, Socket socket, PrintWriter out) {

        String[] parts = line.split("\\s+");
        String cmd = parts[0].toUpperCase();

        switch (cmd) {

            case "REGISTER":
                if (parts.length != 3) {
                    out.println("REJECT format gresit");
                    return;
                }

                int id = Integer.parseInt(parts[1]);
                int port = Integer.parseInt(parts[2]);
                String ip = socket.getInetAddress().getHostAddress();

                if (nodes.containsKey(id)) {
                    out.println("REJECT ID deja folosit");
                    return;
                }

                NodeInfo info = new NodeInfo(id, ip, port, socket, out);
                nodes.put(id, info);

                out.println("ACCEPT " + id);
                broadcastList();
                break;

            case "PONG":
                updatePong(socket);
                break;

            case "LEADER":
                if (parts.length == 2) {
                    try {
                        int leaderId = Integer.parseInt(parts[1]);
                        currentLeader = leaderId;
                        broadcast("LEADER " + leaderId);
                    } catch (Exception ignored) {}
                }
                break;

            default:
                // Alte comenzi ignorate
                break;
        }
    }

    private void updatePong(Socket s) {
        for (NodeInfo n : nodes.values()) {
            if (n.socket == s) {
                n.lastPong = System.currentTimeMillis();
                n.alive = true;
                break;
            }
        }
    }

    private void removeSocket(Socket s) {
        nodes.values().removeIf(n -> n.socket == s);
        broadcastList();
    }

    private void pingLoop() {
        while (true) {
            try {
                Thread.sleep(3000);

                for (NodeInfo n : nodes.values()) {
                    try {
                        n.out.println("PING");
                    } catch (Exception ignored) {}
                }

                // verificare timeout PONG
                long now = System.currentTimeMillis();
                for (NodeInfo n : nodes.values()) {
                    if (now - n.lastPong > 8000) {
                        System.out.println("Nod expirat: " + n);
                        nodes.remove(n.id);
                    }
                }

                broadcastList();

            } catch (InterruptedException ignored) {}
        }
    }

    private void broadcast(String msg) {
        for (NodeInfo n : nodes.values()) {
            try {
                n.out.println(msg);
            } catch (Exception ignored) {}
        }
    }

    private void broadcastList() {
        StringBuilder sb = new StringBuilder();
        sb.append("LIST");

        for (NodeInfo n : nodes.values()) {
            sb.append(" ").append(n.id).append("@").append(n.ip).append(":").append(n.port);
        }

        broadcast(sb.toString());
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 1) {
            System.out.println("Utilizare: java BullyRegistryServer <port>");
            return;
        }

        int p = Integer.parseInt(args[0]);
        new BullyRegistryServer(p).start();
    }
}
