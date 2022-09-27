package ok.dht.test.slastin;

import ok.dht.test.slastin.lsm.DaoException;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.http.HttpSession;
import one.nio.http.Param;
import one.nio.http.Path;
import one.nio.http.Request;
import one.nio.http.Response;
import one.nio.net.Session;

import java.io.IOException;

public class SladkiiHttpServer extends HttpServer {
    static final Response BAD_REQUEST = new Response(Response.BAD_REQUEST, Response.EMPTY);
    static final Response NOT_FOUND = new Response(Response.NOT_FOUND, Response.EMPTY);
    static final Response CREATED = new Response(Response.CREATED, Response.EMPTY);
    static final Response ACCEPTED = new Response(Response.ACCEPTED, Response.EMPTY);
    static final Response INTERNAL_ERROR = new Response(Response.INTERNAL_ERROR, Response.EMPTY);

    private final SladkiiComponent component;

    public SladkiiHttpServer(
            final HttpServerConfig httpServerConfig,
            final SladkiiComponent component
    ) throws IOException {
        super(httpServerConfig);
        this.component = component;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        session.sendResponse(BAD_REQUEST);
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
        try {
            return switch (request.getMethod()) {
                case Request.METHOD_GET -> component.get(id);
                case Request.METHOD_PUT -> component.put(id, request);
                case Request.METHOD_DELETE -> component.delete(id);
                default -> BAD_REQUEST;
            };
        } catch (DaoException e) {
            return INTERNAL_ERROR;
        }
    }
}
