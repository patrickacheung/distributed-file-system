import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.*;

public class ACIDWorker implements Runnable {
    private final Socket client;
    private final ConcurrentHashMap<String, ReadWriteLock> commitedFilesRegistry;
    private final ConcurrentHashMap<Long, Long> txnRegistry; /* 0 = commited, 1 = expired, 2 = abort */ //Might need to log this to a file... // just server crashes left
    private final ConcurrentHashMap<Long, String> txnFileRegistry;
    private final ConcurrentHashMap<Long, ReadWriteLock> txnFileLockRegistry;
    private final String dir;
    private final Path txnsPath;
    private final ReadWriteLock txnStatusLogLock;
    private long txnID;

    public ACIDWorker(Socket client, ConcurrentHashMap<String, ReadWriteLock> commitedFilesRegistry,
                      ConcurrentHashMap<Long, Long> txnRegistry, ConcurrentHashMap<Long, String> txnFileRegistry,
                      ConcurrentHashMap<Long, ReadWriteLock> txnFileLockRegistry, String dir, Path txnsPath,
                      ReadWriteLock txnStatusLogLock) {
        this.client = client;
        this.commitedFilesRegistry = commitedFilesRegistry;
        this.txnRegistry = txnRegistry;
        this.txnFileRegistry = txnFileRegistry;
        this.txnFileLockRegistry = txnFileLockRegistry;
        this.dir = dir;
        this.txnsPath = txnsPath;
        this.txnStatusLogLock = txnStatusLogLock;
    }

    @Override
    public void run() {
        try {
            client.setSoTimeout(0);
        } catch (SocketException se) {
            System.err.println(clientMsg(client, "Failed to set timeout"));
            
            try {
                if (client != null) {
                    client.close();
                }
            } catch (IOException e) {
                System.err.println(clientMsg(client, "Failed to close socket"));
            }
            return;
        }

        System.out.println(clientMsg(client, "Connected"));

        try (
            BufferedReader br = new BufferedReader(new InputStreamReader(client.getInputStream()));
            DataOutputStream dos = new DataOutputStream(new BufferedOutputStream(client.getOutputStream()));
        ) {
            String clientHeader = br.readLine();
            br.readLine(); // Consume linebreak;
            String payload = br.readLine();

            if (clientHeader == null || "".equals(clientHeader)) {
                return;
            }
            
            Request request = getRequest(clientHeader, payload);
            if (request == null) {
                Response response = genErrResponse(txnID);
                dos.writeBytes(response.toString() + "\n");
                dos.flush();
                return;
            }

            // Start txns.
            Response response;
            switch (request.getMethod()) {
                case READ:
                    response = getReadResponse(txnID, request.getData());
                    break;
                case NEW_TXN:
                    response = getNewTxnResponse(request);
                    break;
                case WRITE:
                    response = getWriteResponse(txnID, request);
                    break;
                case ABORT:
                    response = getAbortResponse(txnID, request);
                    break;
                case COMMIT:
                    response = getCommitResponse(txnID, request);
                    break;
                default:
                    response = genErrResponse(txnID);
            }
            dos.writeBytes(response.toString() + "\n");
            dos.flush();
        } catch (IOException e) {
            System.err.println(clientMsg(client, "I/O error"));
        } finally {
            System.out.println(clientMsg(client, "Socket closed"));
        }
    }

    private String clientMsg(Socket client, String msg) {
        return "[" + client.getRemoteSocketAddress().toString() + "] : " + msg;
    }

    private Request getRequest(String header, String payload) {
        // Four parts of header.
        Method method;
        long txID;
        int seqNum;
        int contentLen;

        // Parse header.
        String[] headerComponents = header.split(" ");
        if (headerComponents.length != 4) {
            return null;
        }

        // headerComponent[i]; where i = ...
        // 0 = method; 1 = txID; 2 = seqNum; 3 = contentLen
        method = Method.valueOf(headerComponents[0]);
        // Need to check txid in hashmap.
        // NEW_TXN command has to have txID = -1
        long ltxID = Long.parseLong(headerComponents[1]);
        txnID = ltxID;
        if (ltxID < 0 && (!Method.NEW_TXN.equals(method) && !Method.READ.equals(method))) {
            return null;
        }
        if (Method.NEW_TXN.equals(method) && (ltxID != -1)) {
            return null;
        } else {
            txID = ltxID;
        }

        // NEW_TXN command has to have seqNum = 0
        int lseqNum = Integer.parseInt(headerComponents[2]);
        if (lseqNum < 0) {
            return null;
        }
        if (Method.NEW_TXN.equals(method) && (lseqNum != 0)) {
            return null;
        } else {
            seqNum = lseqNum;
        }

        // Content Length can be > 0 iff there is a payload.
        int lcontentLen = Integer.parseInt(headerComponents[3]);
        if (lcontentLen < 0) {
            return null;
        }
        if ("".equals(payload) && lcontentLen > 0) {
            return null;
        } else { //lines.size() == 2
            contentLen = lcontentLen;
        }

        if ("".equals(payload)) {
            return new Request(method, txID, seqNum, contentLen, null);
        } else {
            return new Request(method, txID, seqNum, contentLen, payload);
        }
    }

    private Response genErrResponse(long txnID) {
        return new Response(Method.ERROR, txnID, -1, Error.INVALID_OP);
    }

    private Response getReadResponse(long txnID, String filename) {
        ReadWriteLock rwl = commitedFilesRegistry.get(filename);
        if (rwl == null) {
            System.err.println(clientMsg(client, filename + " does not exist."));
            return new Response(Method.ERROR, txnID, -1, Error.FILE_NOT_FND);
        }

        File fp = new File(dir + "/" + filename);
        String data = "";
        rwl.readLock().lock();
        try(
            BufferedReader br = new BufferedReader(new FileReader(fp));
        ) {
            String buffer = "";
            while ((buffer = br.readLine()) != null) {
                data += buffer;
            }
            System.out.println(clientMsg(client, "Successfully read " + filename));
        } catch(IOException e) {
            System.err.println(clientMsg(client, "I/O Error trying to read " + filename));
            return new Response(Method.ERROR, txnID, -1, Error.FILE_IO_ERR);
        } finally {
            rwl.readLock().unlock();
        }

        return new Response(Method.READ, txnID, -1, data);
    }

    private Response getNewTxnResponse(Request request) {
        long newtxnID = Server.genTxID();
        long txnStartTime = Server.getEpochSecond();
        String fn = newtxnID + "-" + request.getData();
        
        // Add to registry.
        txnRegistry.putIfAbsent(newtxnID, txnStartTime);
        txnFileRegistry.putIfAbsent(newtxnID, fn);

        // Create a hidden txn file.
        Path fp = Paths.get(txnsPath.toString(), fn);
        try {
            Files.createFile(fp);
        } catch (IOException e) {
            System.err.println(clientMsg(client, "Failed to create " + fp.toString()));
            return new Response(Method.ERROR, newtxnID, -1, Error.FILE_IO_ERR);
        }

        System.out.println(clientMsg(client, "Successfully created " + fp.toString()));
        txnFileLockRegistry.putIfAbsent(newtxnID, new ReentrantReadWriteLock());

        return new Response(Method.ACK, newtxnID, -1, Long.toString(newtxnID));
    }

    private Response getWriteResponse(long txnID, Request request) {
        // Check txnID. Return error 201 Error.
        if (!isTxnIDValid(txnID)) {
            System.err.println(clientMsg(client, "Invalid txnID"));
            return new Response(Method.ERROR, txnID, -1, Error.INVALID_TX_ID);
        }

        // Check if already committed.
        Response ceb = checkCommitedExpiredAborted(txnID);
        if (ceb != null) {
            return ceb;
        }

        // File path.
        String fn = txnFileRegistry.get(txnID);
        // For concurrency.
        if (fn == null) {
            return checkCommitedExpiredAborted(txnID);
        }
        Path fp = Paths.get(txnsPath.toString() + "/" + fn);

        // Get lock.
        ReadWriteLock rwl = txnFileLockRegistry.get(txnID);
        // For concurrency.
        if (rwl == null) {
            return checkCommitedExpiredAborted(txnID);
        }

        // Grab read lock.
        rwl.readLock().lock();
        try(
            BufferedReader br = new BufferedReader(new FileReader(fp.toString()));
        ) {
            List<String> seqNums = getSeqNums(br);

            // Check if txn exists.
            if (seqNums != null) {
                if (seqNums.contains(Integer.toString(request.getSeqNum()))) {
                    System.err.println(clientMsg(client, "Seq num " + Integer.toString(request.getSeqNum())) +
                        " already exists.");
                    return new Response(Method.ERROR, txnID, -1, Error.INVALID_OP);
                }
            }
        } catch (IOException e) {
            System.err.println(clientMsg(client, "Failed to read " + fp.toString()));
            return new Response(Method.ERROR, txnID, -1, Error.FILE_IO_ERR);
        } finally {
            rwl.readLock().unlock();
        }

        // Grab write lock.
        rwl.writeLock().lock();
        /* Write to file in a nice format.
        *  Format:
        *  =====
        *  txnID
        *  msg
        *  =====
        */
        List<String> content = Arrays.asList(
            "=====",
            Integer.toString(request.getSeqNum()),
            request.getData(),
            "====="
        );
        try {
            // Adapted from: https://stackoverflow.com/a/44301865
            Files.write(fp, content, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } catch(IOException e) {
            System.err.println(clientMsg(client, "Failed to write " + fp.toString()));
            return new Response(Method.ERROR, txnID, -1, Error.FILE_IO_ERR);
        } finally {
            rwl.writeLock().unlock();
        }
        System.out.println(clientMsg(client, "Successfully wrote to " + fp.toString()));

        return new Response(Method.WRITE, txnID, -1, "OK");
    }

    private boolean isTxnIDValid(long txnID) {
        return (txnRegistry.get(txnID) != null);
    }

    private List<String> getSeqNums(BufferedReader br) throws IOException {
        List<String> contents = new ArrayList<>();
        List<String> seqNums = new ArrayList<>();
        String buffer;
        int breakCount = 0;
        while ((buffer = br.readLine()) != null) {
            if ("=====".equals(buffer)) {
                ++breakCount;
            } else {
                contents.add(buffer);
            }

            if (breakCount % 2 == 0) {
                seqNums.add(contents.get(0));
                contents.clear();
            }
        }

        if (seqNums.size() == 0) {
            return null;
        }
        return seqNums;
    }

    private Response getAbortResponse(long txnID, Request request) {
        // Check if txnID is valid.
        if (!isTxnIDValid(txnID)) {
            System.err.println(clientMsg(client, "Invalid txnID"));
            return new Response(Method.ERROR, txnID, -1, Error.INVALID_TX_ID);
        }

        // Check if txn already commited/expired/aborted.
        Response ceb = checkCommitedExpiredAborted(txnID);
        if (ceb != null) {
            return ceb;
        }

        // File path.
        String fn = txnFileRegistry.get(txnID);
        // For concurrency.
        if (fn == null) {
            return checkCommitedExpiredAborted(txnID);
        }
        Path fp = Paths.get(txnsPath.toString() + "/" + fn); 

        // Get lock.
        ReadWriteLock rwl = txnFileLockRegistry.get(txnID);
        // For concurrency.
        if (rwl == null) {
            return checkCommitedExpiredAborted(txnID);
        }

        // Delete file.
        rwl.writeLock().lock();
        try {
            Files.delete(fp);

            // Mark as aborted.
            txnRegistry.replace(txnID, 2L);
            // Remove entry from txnFileRegistry.
            txnFileRegistry.remove(txnID);
            // Remove entry from txnFileLockRegistry.
        txnFileLockRegistry.remove(txnID);
        } catch (IOException e) {
            // Throw IOException if IO error.
            System.err.println(clientMsg(client, "Failed to abort " + fp.toString()));
            return new Response(Method.ERROR, txnID, -1, Error.FILE_IO_ERR);
        } finally {
            rwl.writeLock().unlock();
        }

        System.out.println(clientMsg(client, "Successfully aborted " + fp.toString()));
        return new Response(Method.ACK, txnID, -1, "OK");
    }

    private Response getCommitResponse(long txnID, Request request) {
        // Check if txnID is valid.
        if (!isTxnIDValid(txnID)) {
            System.err.println(clientMsg(client, "Invalid txnID"));
            return new Response(Method.ERROR, txnID, -1, Error.INVALID_TX_ID);
        }

        // Check if txn already commited/expired/aborted.
        Response ceb = checkCommitedExpiredAborted(txnID);
        if (ceb != null) {
            return ceb;
        }

        // File path.
        String fn = txnFileRegistry.get(txnID);
        // For concurrency.
        if (fn == null) {
            return checkCommitedExpiredAborted(txnID);
        }
        Path fp = Paths.get(txnsPath.toString() + "/" + fn); 

        // Get lock.
        ReadWriteLock rwl = txnFileLockRegistry.get(txnID);
        // For concurrency.
        if (rwl == null) {
            return checkCommitedExpiredAborted(txnID);
        }

        // Grab read lock.
        rwl.readLock().lock();
        List<String> seqNums;
        try(
            BufferedReader br = new BufferedReader(new FileReader(fp.toString()));
        ) {
            seqNums = getSeqNums(br);
        } catch (IOException e) {
            System.err.println(clientMsg(client, "Failed to read " + fp.toString()));
            return new Response(Method.ERROR, txnID, -1, Error.FILE_IO_ERR);
        } finally {
            rwl.readLock().unlock();
        }

        // Validate and ASK_RESEND && LOG.
        if (seqNums != null) {
            Collections.sort(seqNums);

            int n = Integer.parseInt(seqNums.get(0));
            for (int i = 1; i < seqNums.size(); ++i) {
                int eval = Integer.parseInt(seqNums.get(i));
                if ((eval - n) != 1) {
                    System.err.println(clientMsg(client, "Missing a sequence number " + (i+1)));
                    return new Response(Method.ASK_RESEND, txnID, i+1, "Missing sequence number " + (i+1));
                } else {
                    n = eval;
                }
            }
            if ((request.getSeqNum() - n) != 0) {
                System.err.println(clientMsg(client, "Missing a sequence number " + (n+1)));
                return new Response(Method.ASK_RESEND, txnID, n+1, "Missing sequence number " + (n+1));
            }
        }
        if (seqNums == null && request.getSeqNum() > 0) {
            System.err.println(clientMsg(client, "Missing a sequence number " + 1));
                return new Response(Method.ASK_RESEND, txnID, 1, "Missing sequence number " + 1);
        } 

        // Grab read lock.
        rwl.readLock().lock();
        // Remove logging data from tmp file.
        List<String> content;
        try (
            BufferedReader br = new BufferedReader(new FileReader(fp.toString()));
        ) {
            if (seqNums == null && request.getSeqNum() == 0) {
                content = Arrays.asList("");
            } else {
                content = transformLogToFile(br);
            }
        } catch (IOException e) {
            System.err.println(clientMsg(client, "Failed to read " + fp.toString()));
            return new Response(Method.ERROR, txnID, -1, Error.FILE_IO_ERR);
        } finally {
            rwl.readLock().unlock();
        }

        rwl.writeLock().lock();
        String[] fnarr = fn.split("-");
        Path fp2 = Paths.get(dir + "/" + fnarr[1]);
        try {
            // Write to new file.
            Files.write(fp2, content, StandardCharsets.UTF_8,
                Files.exists(fp2) ? StandardOpenOption.APPEND : StandardOpenOption.CREATE);
            
            // Create new entry in commitedFilesRegistry.
            commitedFilesRegistry.putIfAbsent(fnarr[1], new ReentrantReadWriteLock());
            // Mark txnRegistry to committed.
            txnRegistry.replace(txnID, 0L);
            // Remove entry in txnFileRegistry.
            txnFileRegistry.remove(txnID);
            // Remove entry in txnFileLockRegistry.
            txnFileLockRegistry.remove(txnID);
        } catch (IOException e) {
            System.err.println(clientMsg(client, "Failed to write " + fp2.toString()));
            return new Response(Method.ERROR, txnID, -1, Error.FILE_IO_ERR);
        } finally {
            rwl.writeLock().unlock();
        }

        System.out.println(clientMsg(client, "Successfully wrote to " + fp2.toString()));
        return new Response(Method.ACK, txnID, -1, "OK");
    }

    private List<String> transformLogToFile(BufferedReader br) throws IOException {
        List<String> contentBlock = new ArrayList<>();
        List<String> content = new ArrayList<>();
        String buffer;
        int breakCount = 0;
        while ((buffer = br.readLine()) != null) {
            if ("=====".equals(buffer)) {
                ++breakCount;
            } else {
                contentBlock.add(buffer);
            }

            if (breakCount % 2 == 0) {
                // Not i=0 because 0th is sequence number.
                for (int i = 1; i < contentBlock.size(); ++i) {
                    content.add(contentBlock.get(i));
                }
                contentBlock.clear();
            }
        }

        return content;
    }

    private Response checkCommitedExpiredAborted(long txnID) {
        long timestamp = txnRegistry.get(txnID);

        if (timestamp == 0) {
            System.err.println(clientMsg(client, "txnID " + txnID + " has already commited"));
            return new Response(Method.ACK, txnID, -1, "Already committed.");
        } else if(timestamp == 1) {
            System.err.println(clientMsg(client, "txnID " + txnID + " has already expired"));
            return new Response(Method.ERROR, txnID, -1, Error.INVALID_OP);
        } else if (timestamp == 2) {
            System.err.println(clientMsg(client, "txnID " + txnID + " has already aborted"));
            return new Response(Method.ERROR, txnID, -1, Error.INVALID_OP);
        }

        return null;
    }
}
