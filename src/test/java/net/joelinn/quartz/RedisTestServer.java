package net.joelinn.quartz;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * A single Redis server (docker container) shared by all tests of a JVM. Requires the docker CLI and a running
 * Docker daemon. The image can be overridden with -Dtest.redis.image=... (default: redis:7-alpine).
 *
 * Plain docker CLI is used on purpose: this keeps the test classpath free of extra libraries that would force
 * newer versions of the library's runtime dependencies.
 */
public final class RedisTestServer {
    private static final String LABEL = "quartz-redis-jobstore-test=true";
    private static String host = "localhost";
    private static int port = -1;
    private static String containerId;

    private RedisTestServer() {}

    private static synchronized void start() {
        if (containerId != null) {
            return;
        }
        try {
            String image = System.getProperty("test.redis.image", "redis:7-alpine");
            // the forked test JVM may exit without running shutdown hooks: remove containers left over from earlier runs
            for (String old : docker("ps", "-aq", "--filter", "label=" + LABEL).trim().split("\\s+")) {
                if (!old.isEmpty()) {
                    docker("rm", "-f", old);
                }
            }
            // the first run may print image pull progress; the container id is the last line
            String[] lines = docker("run", "-d", "--rm", "--label", LABEL, "-p", "127.0.0.1::6379", image).trim().split("\n");
            containerId = lines[lines.length - 1].trim();
            final String id = containerId;
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    docker("rm", "-f", id);
                } catch (Exception ignored) {
                    // best effort
                }
            }));
            // output looks like "127.0.0.1:55012"
            String mapping = docker("port", containerId, "6379/tcp").trim().split("\n")[0];
            port = Integer.parseInt(mapping.substring(mapping.lastIndexOf(':') + 1));
            waitUntilReady();
        } catch (Exception e) {
            containerId = null;
            throw new IllegalStateException("Could not start Redis in docker: " + e.getMessage(), e);
        }
    }

    private static void waitUntilReady() throws IOException, InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < end) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 500);
                socket.getOutputStream().write("PING\r\n".getBytes(StandardCharsets.US_ASCII));
                byte[] buf = new byte[16];
                int n = socket.getInputStream().read(buf);
                if (n > 0 && new String(buf, 0, n, StandardCharsets.US_ASCII).startsWith("+PONG")) {
                    return;
                }
            } catch (IOException ignored) {
                // not ready yet
            }
            Thread.sleep(100);
        }
        throw new IOException("Redis did not become ready in time");
    }

    private static String docker(String... args) throws IOException, InterruptedException {
        String[] cmd = new String[args.length + 1];
        cmd[0] = "docker";
        System.arraycopy(args, 0, cmd, 1, args.length);
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        String out = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (p.waitFor() != 0) {
            throw new IOException(Arrays.toString(cmd) + " failed: " + out);
        }
        return out;
    }

    public static String host() {
        start();
        return host;
    }

    public static int port() {
        start();
        return port;
    }
}
