package ok.dht.test.slastin.lsm.exception;

public class StorageClosed extends DaoException {

    public StorageClosed(Throwable causedBy) {
        super(causedBy);
    }

}
