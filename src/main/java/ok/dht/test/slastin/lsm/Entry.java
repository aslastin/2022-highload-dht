package ok.dht.test.slastin.lsm;

public interface Entry<D> {
    D key();

    D value();

    default boolean isTombstone() {
        return value() == null;
    }
}