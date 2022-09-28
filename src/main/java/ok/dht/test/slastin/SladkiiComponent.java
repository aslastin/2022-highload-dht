package ok.dht.test.slastin;

import jdk.incubator.foreign.MemorySegment;
import ok.dht.test.slastin.lsm.BaseEntry;
import ok.dht.test.slastin.lsm.Config;
import ok.dht.test.slastin.lsm.MemorySegmentDao;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.util.Utf8;

import java.io.IOException;

import static ok.dht.test.slastin.SladkiiHttpServer.accepted;
import static ok.dht.test.slastin.SladkiiHttpServer.created;
import static ok.dht.test.slastin.SladkiiHttpServer.notFound;

public class SladkiiComponent {

    private final MemorySegmentDao dao;

    public SladkiiComponent(final Config daoConfig) throws IOException {
        dao = new MemorySegmentDao(daoConfig);
    }

    public Response get(final String id) {
        var entry = dao.get(toMemorySegment(id));
        return entry == null ? notFound() : new Response(Response.OK, entry.value().toByteArray());
    }

    public Response put(final String id, final Request request) {
        var entry = new BaseEntry<>(toMemorySegment(id), toMemorySegment(request.getBody()));
        dao.upsert(entry);
        return created();
    }

    public Response delete(final String id) {
        var entry = new BaseEntry<>(toMemorySegment(id), null);
        dao.upsert(entry);
        return accepted();
    }

    private static MemorySegment toMemorySegment(final String val) {
        return MemorySegment.ofArray(Utf8.toBytes(val));
    }

    private static MemorySegment toMemorySegment(final byte[] bytes) {
        return MemorySegment.ofArray(bytes);
    }
}
