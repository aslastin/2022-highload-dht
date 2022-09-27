package ok.dht.test.slastin.lsm;

public class StorageClosedException extends DaoException {

    public StorageClosedException(Throwable causedBy) {
        super(causedBy);
    }

}
