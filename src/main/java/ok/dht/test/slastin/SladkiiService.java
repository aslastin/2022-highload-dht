package ok.dht.test.slastin;

import ok.dht.Service;
import ok.dht.ServiceConfig;
import ok.dht.test.ServiceFactory;
import ok.dht.test.slastin.lsm.Config;
import one.nio.http.HttpServer;
import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

public class SladkiiService implements Service {
    public static Path DEFAULT_DAO_DIRECTORY = Path.of("dao");
    public static long DEFAULT_FLUSH_THRESHOLD_BYTES = 4 * 1024 * 1024; // 4 Mb

    private final ServiceConfig serviceConfig;
    private final Config daoConfig;

    private HttpServer server;

    public SladkiiService(final ServiceConfig serviceConfig) {
        this(serviceConfig, new Config(
                serviceConfig.workingDir().resolve(DEFAULT_DAO_DIRECTORY),
                DEFAULT_FLUSH_THRESHOLD_BYTES)
        );
    }

    public SladkiiService(final ServiceConfig serviceConfig, final Config daoConfig) {
        this.serviceConfig = serviceConfig;
        this.daoConfig = daoConfig;
    }

    @Override
    public CompletableFuture<?> start() throws IOException {
        var httpServerConfig = makeHttpServerConfig(serviceConfig.selfPort());
        var component = makeComponent();
        server = new SladkiiHttpServer(httpServerConfig, component);
        server.start();
        return CompletableFuture.completedFuture(null);
    }

    private SladkiiComponent makeComponent() throws IOException {
        var daoDirectoryPath = daoConfig.basePath();
        if (Files.notExists(daoDirectoryPath)) {
            Files.createDirectories(daoDirectoryPath);
        }
        return new SladkiiComponent(daoConfig);
    }

    @Override
    public CompletableFuture<?> stop() {
        server.stop();
        server = null;
        return CompletableFuture.completedFuture(null);
    }

    private static HttpServerConfig makeHttpServerConfig(int port) {
        HttpServerConfig httpConfig = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = port;
        acceptor.reusePort = true;
        httpConfig.acceptors = new AcceptorConfig[]{acceptor};
        return httpConfig;
    }

    @ServiceFactory(stage = 1, week = 1, bonuses = "SingleNodeTest#respectFileFolder")
    public static class Factory implements ServiceFactory.Factory {

        @Override
        public Service create(ServiceConfig config) {
            return new SladkiiService(config);
        }
    }
}
