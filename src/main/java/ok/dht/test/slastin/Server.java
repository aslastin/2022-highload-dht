package ok.dht.test.slastin;

import ok.dht.ServiceConfig;

import java.nio.file.Files;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

public final class Server {

    private Server() {
        // Only main method
    }

    public static void main(String[] args) throws Exception {
        int port = 2022;
        String url = "http://localhost:" + port;
        ServiceConfig cfg = new ServiceConfig(
                port,
                url,
                Collections.singletonList(url),
                Files.createTempDirectory("server")
        );
        new SladkiiService(cfg).start().get(1, TimeUnit.SECONDS);
        System.out.println("Server is located by " + url);
    }
}
