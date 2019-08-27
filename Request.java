public class Request {
    private final Method method;
    private final long txID;
    private final int seqNum;
    private final int contentLen; // In bytes.
    private final String data;

    public Request(Method method, long txID, int seqNum, int contentLen, String data) {
        this.method = method;
        this.txID = txID;
        this.seqNum = seqNum;
        this.contentLen = contentLen;
        this.data = data;
    }

    public Method getMethod() {
        return method;
    }

    public long getTxID() {
        return txID;
    }

    public int getSeqNum() {
        return seqNum;
    }

    public int getContentLen() {
        return contentLen;
    }

    public String getData() {
        return data;
    }

    @Override
    public String toString() {
        final String blankline = "\r\n";
        final String header = method.toString() + " " + txID + " " + seqNum + 
                              " " + contentLen;
        
        final String request;
        if (data != null) {
            request = header + blankline + blankline + data;
        } else {
            request = header + blankline + blankline + blankline;
        }

        return request;
    }
}
