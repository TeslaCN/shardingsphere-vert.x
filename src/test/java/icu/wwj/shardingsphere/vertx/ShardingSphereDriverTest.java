package icu.wwj.shardingsphere.vertx;

import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

public final class ShardingSphereDriverTest {
    
    @Test
    public void assertParseConnectionUriWithClasspath() {
        ShardingSphereOptions actual = (ShardingSphereOptions) new ShardingSphereDriver().parseConnectionUri("shardingsphere:classpath:/test.yaml");
        assertThat(new String(actual.getYamlConfigurationBytes()), is("databaseName: test\n"));
    }
    
    @Test
    public void assertParseConnectionUriWithAbsolutePath() throws IOException {
        Path tmpDir = Paths.get("/tmp");
        assumeTrue(Files.exists(tmpDir) && Files.isDirectory(tmpDir));
        Path configurationFile = Paths.get("/tmp/" + System.nanoTime());
        String expected = "databaseName: test\n";
        try {
            Files.write(configurationFile, expected.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            ShardingSphereOptions actual = (ShardingSphereOptions) new ShardingSphereDriver().parseConnectionUri("shardingsphere:" + configurationFile);
            assertThat(new String(actual.getYamlConfigurationBytes()), is(expected));
        } finally {
            Files.deleteIfExists(configurationFile);
        }
    }
}
