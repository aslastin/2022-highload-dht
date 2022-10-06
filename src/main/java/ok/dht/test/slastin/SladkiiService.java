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
import java.util.function.Function;
import java.util.function.Supplier;

public class SladkiiService implements Service {
    public static final Path DEFAULT_DB_DIRECTORY = Path.of("db");
    public static final long DEFAULT_MEMTABLE_SIZE = 8 * SizeUnit.MB;

    public static final Supplier<Options> DEFAULT_OPTIONS_SUPPLIER = () -> new Options()
            .setCreateIfMissing(true)
            .setWriteBufferSize(DEFAULT_MEMTABLE_SIZE)
            .setLevelCompactionDynamicLevelBytes(true);

    public static final Function<ServiceConfig, HttpServerConfig> DEFAULT_HTTP_CONFIG_MAPPER = cfg -> {
        HttpServerConfig httpConfig = new HttpServerConfig();
        AcceptorConfig acceptor = new AcceptorConfig();
        acceptor.port = cfg.selfPort();
        acceptor.reusePort = true;
        httpConfig.acceptors = new AcceptorConfig[]{acceptor};
        return httpConfig;
    };

    private final ServiceConfig serviceConfig;
    private final Supplier<Options> dbOptionsSupplier;
    private final Function<ServiceConfig, HttpServerConfig> httpConfigMapper;

    private Options dbOptions;
    private SladkiiComponent component;
    private SladkiiServer server;

    private boolean isClosed;

    public SladkiiService(ServiceConfig serviceConfig) {
        this(serviceConfig, DEFAULT_OPTIONS_SUPPLIER, DEFAULT_HTTP_CONFIG_MAPPER);
    }

    public SladkiiService(
            ServiceConfig serviceConfig,
            Supplier<Options> dbOptionsSupplier,
            Function<ServiceConfig, HttpServerConfig> httpConfigSupplier
    ) {
        this.serviceConfig = serviceConfig;
        this.dbOptionsSupplier = dbOptionsSupplier;
        this.httpConfigMapper = httpConfigSupplier;
    }

    @Override
    public CompletableFuture<?> start() throws IOException {
        isClosed = false;

        dbOptions = dbOptionsSupplier.get();
        component = makeComponent(dbOptions, serviceConfig.workingDir());

        server = new SladkiiServer(httpConfigMapper.apply(serviceConfig), component);
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

    @ServiceFactory(stage = 1, week = 1, bonuses = "SingleNodeTest#respectFileFolder")
    public static class Factory implements ServiceFactory.Factory {

        @Override
        public Service create(ServiceConfig config) {
            return new SladkiiService(config);
        }
    }
}
