import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Stock Hadoop DFSClient against Pillar: the code path `hadoop fs` uses. */
public class HdfsInterop {
    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
        System.out.println("ok    " + what);
    }

    /** Written before the NameNode is killed, checked after it restarts. */
    static final Path KEEP = new Path("/keep/data.bin");

    static byte[] keepData() {
        byte[] data = new byte[(7 << 19) + 123];
        new Random(7).nextBytes(data);
        return data;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", args[0]);
        conf.set("dfs.replication", "3");
        conf.setLong("dfs.blocksize", 1L << 20);
        FileSystem fs = FileSystem.get(conf);
        if (args.length > 1 && args[1].equals("verify")) {
            verifyAfterRestart(fs);
            return;
        }

        Path dir = new Path("/java/nested");
        check(fs.mkdirs(dir), "mkdirs -p " + dir);

        // A 2.5-block file through a three-DataNode pipeline.
        byte[] data = new byte[5 << 19];
        new Random(42).nextBytes(data);
        Path f = new Path(dir, "f.bin");
        try (FSDataOutputStream out = fs.create(f)) {
            out.write(data);
        }
        FileStatus st = fs.getFileStatus(f);
        check(st.getLen() == data.length, "create+write+close, length " + st.getLen());
        check(st.getReplication() == 3, "replication " + st.getReplication());

        byte[] back = new byte[data.length];
        try (FSDataInputStream in = fs.open(f)) {
            in.readFully(back);
        }
        check(Arrays.equals(back, data), "sequential read is byte-identical");

        byte[] mid = new byte[3000];
        try (FSDataInputStream in = fs.open(f)) {
            in.readFully((1 << 20) - 1000, mid);
        }
        check(Arrays.equals(mid, Arrays.copyOfRange(data, (1 << 20) - 1000, (1 << 20) + 2000)),
              "positional read across a block boundary");

        // What `hadoop fs -put` does: copy to ._COPYING_, then rename.
        File local = File.createTempFile("pillar", ".txt");
        Files.writeString(local.toPath(), "hello from the JVM\n");
        fs.copyFromLocalFile(new Path(local.getAbsolutePath()), new Path("/java/put.txt"));
        try (FSDataInputStream in = fs.open(new Path("/java/put.txt"))) {
            byte[] b = in.readAllBytes();
            check(new String(b).equals("hello from the JVM\n"), "copyFromLocalFile (-put) and read back (-cat)");
        }

        FileStatus[] ls = fs.listStatus(new Path("/java"));
        check(ls.length == 2, "listStatus /java has " + ls.length + " entries");

        check(fs.rename(f, new Path("/java/renamed.bin")), "rename");
        check(!fs.exists(f) && fs.exists(new Path("/java/renamed.bin")), "exists after rename");

        try (FSDataOutputStream out = fs.create(new Path("/java/put.txt"), true)) {
            out.write("replaced\n".getBytes());
        }
        try (FSDataInputStream in = fs.open(new Path("/java/put.txt"))) {
            check(new String(in.readAllBytes()).equals("replaced\n"), "create(overwrite=true) replaces an existing file");
        }

        boolean threw = false;
        try (FSDataOutputStream out = fs.create(new Path("/java/put.txt"), false)) {
        } catch (org.apache.hadoop.fs.FileAlreadyExistsException e) {
            threw = true;
        }
        check(threw, "create(overwrite=false) on an existing file throws FileAlreadyExistsException");

        check(fs.delete(new Path("/java"), true), "recursive delete");
        check(!fs.exists(new Path("/java")), "gone after delete");

        try (FSDataOutputStream out = fs.create(KEEP)) {
            out.write(keepData());
        }
        check(fs.getFileStatus(KEEP).getLen() == keepData().length, "wrote " + KEEP + " for the restart check");
        fs.close();
        System.out.println("ALL OK");
    }

    /** After the NameNode was killed and restarted on the same name dir. */
    static void verifyAfterRestart(FileSystem fs) throws Exception {
        byte[] data = keepData();
        check(fs.getFileStatus(KEEP).getLen() == data.length, "after restart: " + KEEP + " length " + data.length);
        byte[] back = new byte[data.length];
        try (FSDataInputStream in = fs.open(KEEP)) {
            in.readFully(back);
        }
        check(Arrays.equals(back, data), "after restart: content is byte-identical");
        check(!fs.exists(new Path("/java")), "after restart: deleted tree stays deleted");

        Path more = new Path("/keep/after.txt");
        try (FSDataOutputStream out = fs.create(more)) {
            out.write("written after the restart\n".getBytes());
        }
        try (FSDataInputStream in = fs.open(more)) {
            check(new String(in.readAllBytes()).equals("written after the restart\n"), "after restart: new writes work");
        }
        check(fs.delete(new Path("/keep"), true), "after restart: delete");
        fs.close();
        System.out.println("ALL OK after restart");
    }
}
