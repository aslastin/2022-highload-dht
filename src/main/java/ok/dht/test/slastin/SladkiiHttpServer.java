package ok.dht.test.slastin;

import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Session;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SladkiiHttpServer extends HttpServer {
    private static final Response BAD_REQUEST = new Response(Response.BAD_REQUEST, Response.EMPTY);
    private static final Response NOT_FOUND = new Response(Response.NOT_FOUND, Response.EMPTY);
    private static final Response CREATED = new Response(Response.CREATED, Response.EMPTY);
    private static final Response ACCEPTED = new Response(Response.ACCEPTED, Response.EMPTY);

    private final Map<String, byte[]> dao;

    public SladkiiHttpServer(final HttpServerConfig config) throws IOException {
        super(config);
        dao = new HashMap<>();
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
        return dao.containsKey(id) ? new Response(Response.OK, dao.get(id)) : NOT_FOUND;
    }

    public Response put(final String id, final Request request) {
        dao.put(id, request.getBody());
        return CREATED;
    }

    public Response delete(final String id) {
        dao.remove(id);
        return ACCEPTED;
    }
}
