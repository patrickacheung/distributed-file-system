public enum Error {
    INVALID_TX_ID(201, "Invalid transaction ID"),
    INVALID_OP(202, "Invalid operation"),
    FILE_IO_ERR(205, "File I/O error"),
    FILE_NOT_FND(206, "File not found"),
    NONE(0, null);

    private final int code;
    private final String description;

    private Error(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        return code + ": " + description;
    }
}
