import java.io.*;
import java.net.*;
import java.nio.file.*;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.*;
import java.util.concurrent.*;

public class Server {
    private final String address;
    private final int port;
    private final String dir;
    private final AtomicBoolean isStopped;
    private final ExecutorService threadPool;
    private final Path txnsPath;
    private final ConcurrentHashMap<String, ReadWriteLock> commitedFilesRegistry;
    private final ConcurrentHashMap<Long, Long> txnRegistry;
    private final ConcurrentHashMap<Long, String> txnFileRegistry;
    private final ConcurrentHashMap<Long, ReadWriteLock> txnFileLockRegistry;
    private final ReadWriteLock txnStatusLogLock;
    private ServerSocket serverSocket = null;
    public static final long expirySeconds = 60;
    public static final String txnStatusLog = "txn_status_log";

    public Server(String address, int port, String dir) {
        this.address = address;
        this.port = port;
        this.dir = dir;
        commitedFilesRegistry = new ConcurrentHashMap<>();
        txnRegistry = new ConcurrentHashMap<>();
        txnFileRegistry = new ConcurrentHashMap<>();
        txnFileLockRegistry = new ConcurrentHashMap<>();
        txnStatusLogLock = new ReentrantReadWriteLock();
        txnsPath = Paths.get(dir + "/.txns");
        isStopped = new AtomicBoolean(false);
        threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    }

    public void startUp() {
        System.out.println("Starting up server...");

        // Create dir if it doesn't exist.
        if (!Files.isDirectory(Paths.get(dir))) {
            try {
                Files.createDirectory(Paths.get(dir));
            } catch (IOException e) {
                System.err.println("Failed to create dir " + dir);
                System.exit(1);
            }
        } else {
            // Existing files.
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
                for (Path entry : stream) {
                    commitedFilesRegistry.put(entry.getFileName().toString(), new ReentrantReadWriteLock());
                }
            } catch (IOException e) {
                System.err.println("Failed to read existing files.");
            }
        }

        // Create folder for txs.
        if (!Files.isDirectory(txnsPath)) {
            try {
                Files.createDirectory(txnsPath);
            } catch (IOException e) {
                System.err.println("Failed to create internal dir");
                System.exit(1);
            }

            Path fp = Paths.get(txnsPath.toString() + "/" + Server.txnStatusLog);
            try {
                Files.createFile(fp);
                /* will work like this: <txnid>:<timestamp-or-(0...2)>:<filename-without-txnid>*/
            } catch (IOException e) {
                System.err.println("Failed to create " + fp.toString());
                System.exit(1);
            }
        } else {
            // Recovery from non-clean shutdown.
            // similar to below.
            /*try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get(dir))) {
                for (Path entry : stream) {
                    System.out.println(entry);
                }
            } catch (IOException e) {
                System.err.println("Failed to read existing files.");
            }*/
        }

        // Open socket.
        try {
            InetAddress inaddr = InetAddress.getByName(address);
            serverSocket = new ServerSocket(port, 50, inaddr); // Max queue of 50.
        } catch (UnknownHostException e) {
            System.err.println("Invalid address: " + address);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Failed to open port: " + port);
            System.exit(1);
        }

        System.out.println("Server listening on " + address + ":" + port);

        while (!isStopped.get()) {
            try {
                threadPool.execute(new ACIDWorker(serverSocket.accept(), commitedFilesRegistry,
                                                  txnRegistry, txnFileRegistry, txnFileLockRegistry,
                                                  dir, txnsPath, txnStatusLogLock));
            } catch (IOException e) {
                //System.err.println("Client failed to connect.");
                continue;
            }
        }
    }

    public void shutDown() {
        isStopped.set(true);
        System.out.println("\nCleaning up...");

        // Close socket.
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                System.err.println("Failed to close socket");
            }
        }

        // Shutdown threadpool.
        shutdownAndAwaitTermination(threadPool);

        // Clean txn directory.
        try {
            cleanTxns(txnsPath);
        } catch (NoSuchFileException e) {
            System.err.println("No files to clean");
        } catch (IOException e) {
            System.err.println("Failed to cleanup files");
        }

        System.out.println("Shutting down server...");
    }

    // https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
    private void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted.
        try {
            // Wait 10 sec for existing tasks to terminate.
            if (!pool.awaitTermination(10, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Kill all current tasks.
                if (!pool.awaitTermination(10, TimeUnit.SECONDS)) { // Wait another 10 seconds.
                    System.err.println("Pool failed to terminate");
                }
            }
        } catch (InterruptedException e) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    private void cleanTxns(Path path) throws IOException, NoSuchFileException {
        DirectoryStream<Path> stream = Files.newDirectoryStream(path);
        for (Path entry : stream) {
            if (Files.isDirectory(entry)) {
                cleanTxns(entry);
            }
            Files.delete(entry);
        }
        Files.deleteIfExists(path);
    }

    public static void main(String[] args) {
        final HashSet<String> flags = new HashSet<>();
        flags.add("h");
        flags.add("a");
        flags.add("p");
        flags.add("d");

        final HashSet<String> doubleFlags = new HashSet<>();
        doubleFlags.add("help");
        doubleFlags.add("address");
        doubleFlags.add("port");
        doubleFlags.add("directory");

        // Default params.
        String address = "localhost";
        int port = 8080;
        String dir = "./tmp";

        // Parse args.
        for (int i = 0; i < args.length; ++i) {
            if (args[i].charAt(0) == '-') {
                if (args[i].length() < 2) {
                    printFlagErr(args[i]);
                    System.exit(1);
                } else if (args[i].length() == 2 && args[i].charAt(1) == 'h') {
                    printHelp();
                    System.exit(0);
                } else if (args[i].length() == 6 && args[i].substring(2).equals("help")) {
                    printHelp();
                    System.exit(0);
                }

                if (args.length - 1 == i) {
                    printExpectedArgErr(args[i]);
                    System.exit(1);
                }

                if (args[i].charAt(1) == '-') {
                    if (args[i].length() < 3) {
                        printFlagErr(args[i]);
                        System.exit(1);
                    }

                    String doubleFlag = args[i].substring(2);
                    if (doubleFlags.contains(doubleFlag)) {
                        switch (doubleFlag) {
                            case "address":
                                address = args[i+1];
                                break;
                            case "port":
                                port = Integer.parseInt(args[i+1]);
                                break;
                            case "directory":
                                dir = args[i+1];
                                break;
                        }
                        ++i;
                    } else {
                        printFlagErr(args[i]);
                        System.exit(1);
                    }
                } else {
                    String singleFlag = args[i].substring(1);
                    if (flags.contains(singleFlag)) {
                        switch (singleFlag) {
                            case "a":
                                address = args[i+1];
                                break;
                            case "p":
                                port = Integer.parseInt(args[i+1]);
                                break;
                            case "d":
                                dir = args[i+1];
                                break;
                        }
                        ++i;
                    } else {
                        printFlagErr(args[i]);
                        System.exit(1);
                    }
                }
            } else {
                printFlagErr(args[i]);
                System.exit(1);
            }
        }

        Server server = new Server(address, port, dir);

        // Safe shutdown.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                server.shutDown();
            }
        });

        // Startup Server.
        server.startUp();
    }

    private static void printFlagErr(String flag) {
        final String usage = "Incorrect Usage:\njava Server [-a address] [-p port] [-d dir]\n" + 
                             "or\n" +
                             "java Server [--address address] [--port port] [--directory dir]";
        System.err.println("Invalid flag: " + flag);
        System.err.println(usage);
    }

    private static void printExpectedArgErr(String flag) {
        System.err.println("Expected arg after: " + flag);
    }

    private static void printHelp() {
        final String usage = "Usage:\njava Server [-a address] [-p port] [-d dir]\n" + 
                             "or\n" +
                             "java Server [--address address] [--port port] [--directory dir]";
        System.out.println(usage);
    }

    // Taken from: https://stackoverflow.com/a/18228151
    private static final long LIMIT = 10000000000L;
    private static volatile long last = 0;
    public static synchronized long genTxID() {
        // 10 digits.
        long id = System.currentTimeMillis() % LIMIT;
        if ( id <= last ) {
            id = (last + 1) % LIMIT;
        }
        return last = id;
    }

    public static synchronized long getEpochSecond() {
        return System.currentTimeMillis() / 1000L;
    }
}
