package ok.dht.test.slastin;

import ok.dht.Service;
import ok.dht.ServiceConfig;
import ok.dht.test.ServiceFactory;
import one.nio.http.HttpServerConfig;
import one.nio.server.AcceptorConfig;
import org.rocksdb.Options;
import org.rocksdb.util.SizeUnit;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

public class SladkiiService implements Service {
    public static Path DEFAULT_DB_DIRECTORY = Path.of("db");
    public static long DEFAULT_MEMTABLE_SIZE = 8 * SizeUnit.MB;

    public static Supplier<Options> DEFAULT_OPTIONS_SUPPLIER = () -> new Options()
            .setCreateIfMissing(true)
            .setWriteBufferSize(DEFAULT_MEMTABLE_SIZE)
            .setMaxTotalWalSize(DEFAULT_MEMTABLE_SIZE)
            .setLevelCompactionDynamicLevelBytes(true);

    private final ServiceConfig serviceConfig;
    private final Supplier<Options> dbOptionsSupplier;

    private Options dbOptions;
    private SladkiiComponent component;
    private SladkiiHttpServer server;

    private boolean isClosed;

    public SladkiiService(ServiceConfig serviceConfig) {
        this(serviceConfig, DEFAULT_OPTIONS_SUPPLIER);
    }

    public SladkiiService(ServiceConfig serviceConfig, Supplier<Options> dbOptionsSupplier) {
        this.serviceConfig = serviceConfig;
        this.dbOptionsSupplier = dbOptionsSupplier;
    }

    @Override
    public CompletableFuture<?> start() throws IOException {
        isClosed = false;

        dbOptions = dbOptionsSupplier.get();

        component = makeComponent(dbOptions, serviceConfig.workingDir());

        var httpServerConfig = makeHttpServerConfig(serviceConfig.selfPort());
        server = new SladkiiHttpServer(httpServerConfig, component);

        server.start();

        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<?> stop() {
        if (!isClosed) {
            server.stop();
            server = null;

            component.close();
            component = null;

            dbOptions.close();
            dbOptions = null;

            isClosed = true;
        }

        return CompletableFuture.completedFuture(null);
    }

    private static SladkiiComponent makeComponent(Options dbOptions, Path serverDirectory) throws IOException {
        Path location = serverDirectory.resolve(DEFAULT_DB_DIRECTORY);
        if (Files.notExists(location)) {
            Files.createDirectories(location);
        }
        return new SladkiiComponent(dbOptions, location.toString());
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
