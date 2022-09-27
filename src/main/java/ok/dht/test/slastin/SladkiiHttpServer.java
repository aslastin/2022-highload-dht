package ok.dht.test.slastin;

import jdk.incubator.foreign.MemorySegment;
import ok.dht.test.slastin.lsm.BaseEntry;
import ok.dht.test.slastin.lsm.Config;
import ok.dht.test.slastin.lsm.MemorySegmentDao;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Session;
import one.nio.util.Utf8;

import java.io.IOException;

public class SladkiiHttpServer extends HttpServer {
    private static final Response BAD_REQUEST = new Response(Response.BAD_REQUEST, Response.EMPTY);
    private static final Response NOT_FOUND = new Response(Response.NOT_FOUND, Response.EMPTY);
    private static final Response CREATED = new Response(Response.CREATED, Response.EMPTY);
    private static final Response ACCEPTED = new Response(Response.ACCEPTED, Response.EMPTY);

    private final MemorySegmentDao dao;

    public SladkiiHttpServer(final HttpServerConfig httpServerConfig, final Config daoConfig) throws IOException {
        super(httpServerConfig);
        dao = new MemorySegmentDao(daoConfig);
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Override
    public synchronized void stop() {
        // close sessions with clients
        for (var selectorThread : selectors) {
            selectorThread.selector.forEach(Session::close);
        }
        super.stop();
    }

    @Path("/v0/entity")
    public Response handleRequest(
            @Param(value = "id", required = true) final String id,
            final Request request
    ) {
        if (id.isBlank()) {
            return BAD_REQUEST;
        }
        return switch (request.getMethod()) {
            case Request.METHOD_GET -> get(id);
            case Request.METHOD_PUT -> put(id, request);
            case Request.METHOD_DELETE -> delete(id);
            default -> BAD_REQUEST;
        };
    }

    public Response get(final String id) {
        var entry = dao.get(Utils.toMemorySegment(id));
        return entry != null ? new Response(Response.OK, entry.value().toByteArray()) : NOT_FOUND;
    }

    public Response put(final String id, final Request request) {
        var entry = new BaseEntry<>(Utils.toMemorySegment(id), Utils.toMemorySegment(request.getBody()));
        dao.upsert(entry);
        return CREATED;
    }

    public Response delete(final String id) {
        var entry = new BaseEntry<>(Utils.toMemorySegment(id), null);
        dao.upsert(entry);
        return ACCEPTED;
    }

    private static class Utils {
        public static MemorySegment toMemorySegment(final String val) {
            return MemorySegment.ofArray(Utf8.toBytes(val));
        }

        public static MemorySegment toMemorySegment(final byte[] bytes) {
            return MemorySegment.ofArray(bytes);
        }
    }
}
