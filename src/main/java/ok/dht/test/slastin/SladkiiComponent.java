package ok.dht.test.slastin;

import one.nio.http.Request;
import one.nio.http.Response;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.Closeable;
import java.nio.charset.StandardCharsets;

import static ok.dht.test.slastin.SladkiiHttpServer.*;

public class SladkiiComponent implements Closeable {

    private RocksDB db;

    public SladkiiComponent(Options options, String location) {
        try {
            db = RocksDB.open(options, location);
        } catch (RocksDBException e) {
            // TODO
            System.err.println("Can not open DB: " + e.getMessage());
        }
    }

    public Response get(String id) {
        try {
            byte[] value = db.get(toBytes(id));
            return value == null ? notFound() : new Response(Response.OK, value);
        } catch (RocksDBException e) {
            // TODO
            System.err.println("get");
            return internalError();
        }
    }

    public Response put(String id, Request request) {
        try {
            db.put(toBytes(id), request.getBody());
            return created();
        } catch (RocksDBException e) {
            // todo
            System.err.println("put");
            return internalError();
        }
    }

    public Response delete(String id) {
        try {
            db.delete(toBytes(id));
            return accepted();
        } catch (RocksDBException e) {
            // todo
            System.err.println("put");
            return internalError();
        }
    }

    private static byte[] toBytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void close() {
        if (db != null) {
            db.close();
            db = null;
        }
    }
}
