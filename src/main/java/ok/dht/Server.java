package ok.dht;

import ok.dht.test.slastin.SladkiiService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public final class Server {
    private final static int DEFAULT_SERVER_PORT = 2022;
    private final static String DEFAULT_SERVER_URL = "http://localhost:" + DEFAULT_SERVER_PORT;
    private final static String DEFAULT_SERVER_NAME = "server";

    private Server() {
        // Only main method
    }

    private static Path createServerDirectory(final String serverDirectoryName) throws IOException {
        final Path serverDirectory = Path.of(serverDirectoryName);
        if (Files.notExists(serverDirectory)) {
            Files.createDirectory(serverDirectory);
        }
        return serverDirectory;
    }

    public static void main(String[] args) throws Exception {
        String serverDirectoryName = args.length == 0 ? null : args[0];
        Path serverDirectory = serverDirectoryName == null
                ? Files.createTempDirectory(DEFAULT_SERVER_NAME)
                : createServerDirectory(serverDirectoryName);
        ServiceConfig cfg = new ServiceConfig(
                DEFAULT_SERVER_PORT,
                DEFAULT_SERVER_URL,
                Collections.singletonList(DEFAULT_SERVER_URL),
                serverDirectory
        );
        new SladkiiService(cfg).start().get(1, TimeUnit.SECONDS);
        System.out.println("Server is located by " + DEFAULT_SERVER_URL);
    }
}
