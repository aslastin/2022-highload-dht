package ok.dht.test.slastin.lsm.exception;

public class StorageClosedException extends DaoException {

    public StorageClosedException(Throwable causedBy) {
        super(causedBy);
    }

}
