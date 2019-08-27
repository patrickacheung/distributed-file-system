import java.nio.charset.StandardCharsets;

public class Response {
    private final Method method;
    private final long txID;
    private final int seqNum;
    private final Error err;
    private final int contentLen; // In bytes.
    private final String reason;

    public Response(Method method, long txID, int seqNum, Error err) {
        this.method = method;
        this.txID = txID;
        this.seqNum = seqNum;
        this.err = err;
        this.contentLen = getContentLenFromErr(err);
        this.reason = getReasonFromErr(err);
    }

    public Response(Method method, long txID, int seqNum, String msg) {
        this.method = method;
        this.txID = txID;
        this.seqNum = seqNum;
        this.err = null;
        this.contentLen = getContentLenFromMsg(msg);
        this.reason = msg;
    }

    private String getReasonFromErr(Error e) {
        if (e != null) {
            return e.getDescription();
        }
        return null;
    }

    private int getContentLenFromMsg(String s) {
        return s.getBytes(StandardCharsets.UTF_8).length;
    }

    private int getContentLenFromErr(Error e) {
        if (e != null) {
            return e.getDescription().getBytes(StandardCharsets.UTF_8).length;
        }
        return 0;
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

    public Error getErr() {
        return err;
    }

    public int contentLen() {
        return contentLen;
    }

    public String getReason() {
        return reason;
    }

    @Override
    public String toString() {
        final String blankline = "\r\n";
        final String header;
        if (!method.equals(Method.ERROR)) {
            header = method.toString() + " " + txID + " " + seqNum +
                     " null " + contentLen;
        } else {
            header = method.toString() + " " + txID + " " + seqNum + 
                     " " + err.getCode() + " " + contentLen;
        }

        final String response;
        if (reason != null) {
            response = header + blankline + blankline + reason;
        } else {
            response = header + blankline + blankline + blankline;
        }

        return response;
    }
}
