package ok.dht.test.slastin.lsm;

import java.nio.file.Path;

public record Config(
        Path basePath,
        long flushThresholdBytes) {
}
