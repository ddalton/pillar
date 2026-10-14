import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;

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
        if (args.length > 3 && args[1].equals("pipeline")) {
            pipelineAcrossADatanodeFailure(fs, args[2], args[3]);
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

        // Metadata calls take effect (they used to answer success and do nothing).
        Path m = new Path("/java/renamed.bin");
        check(fs.getFileStatus(m).getOwner().equals(System.getenv("HADOOP_USER_NAME")),
              "owner is the calling user (" + fs.getFileStatus(m).getOwner() + ")");
        fs.setPermission(m, new FsPermission((short) 0640));
        fs.setOwner(m, "bob", "eng");
        fs.setTimes(m, 1_000_000L, 2_000_000L);
        check(fs.setReplication(m, (short) 2), "setReplication");
        FileStatus ms = fs.getFileStatus(m);
        check(ms.getPermission().toShort() == 0640 && ms.getOwner().equals("bob") && ms.getGroup().equals("eng")
              && ms.getModificationTime() == 1_000_000L && ms.getAccessTime() == 2_000_000L && ms.getReplication() == 2,
              "setPermission/setOwner/setTimes/setReplication are visible");
        fs.access(m, FsAction.READ);
        boolean missing = false;
        try { fs.access(new Path("/java/nope"), FsAction.READ); } catch (java.io.FileNotFoundException e) { missing = true; }
        check(missing, "access() on a missing path throws FileNotFoundException");
        ContentSummary cs = fs.getContentSummary(new Path("/java"));
        check(cs.getLength() == data.length + "replaced\n".length() && cs.getFileCount() == 2 && cs.getDirectoryCount() == 2,
              "getContentSummary: " + cs.getLength() + " bytes, " + cs.getFileCount() + " files, " + cs.getDirectoryCount() + " dirs");

        // append: a full last block starts a new one; a partial last block is
        // extended, or left alone with CreateFlag.NEW_BLOCK.
        Path ap = new Path("/java/append.bin");
        byte[] head = new byte[1 << 20];
        new Random(5).nextBytes(head);
        try (FSDataOutputStream out = fs.create(ap)) { out.write(head); }
        try (FSDataOutputStream out = fs.append(ap)) { out.write("tail".getBytes()); }
        check(fs.getFileStatus(ap).getLen() == head.length + 4, "append after a full block");
        // The last block is now partial (1 MiB + 4 bytes): appending extends
        // it in place (pipeline stage APPEND, new generation stamp), the
        // first packet completing a partial checksum chunk.
        try (FSDataOutputStream out = fs.append(ap)) { out.write("x".getBytes()); }
        check(fs.getFileStatus(ap).getLen() == head.length + 5, "append into a partial last block");
        try (FSDataOutputStream out = ((DistributedFileSystem) fs).append(ap,
                EnumSet.of(CreateFlag.APPEND, CreateFlag.NEW_BLOCK), 4096, null)) {
            out.write("more".getBytes());
        }
        byte[] got;
        try (FSDataInputStream in = fs.open(ap)) { got = in.readAllBytes(); }
        check(got.length == head.length + 9 && Arrays.equals(Arrays.copyOf(got, head.length), head)
              && new String(got, head.length, 9).equals("tailxmore"), "append with NEW_BLOCK; content intact");

        // Leases: one writer at a time; recoverLease takes a file from a
        // writer that is gone.
        DistributedFileSystem dfs = (DistributedFileSystem) fs;
        Path lp = new Path("/java/leased.bin");
        FSDataOutputStream held = fs.create(lp);
        held.write(new byte[1000]);   // buffered: no block allocated yet
        check(!dfs.isFileClosed(lp), "isFileClosed is false while the file is being written");
        long id1 = dfs.getClient().getFileInfo(lp.toString()).getFileId();
        long id2 = dfs.getClient().getFileInfo("/java/put.txt").getFileId();
        check(id1 > 0 && id2 > 0 && id1 != id2, "files have distinct, non-zero file ids (" + id1 + ", " + id2 + ")");
        FileSystem other = FileSystem.newInstance(fs.getUri(), conf);   // another client name
        boolean refused2 = false;
        try { other.create(lp, true).close(); }
        catch (java.io.IOException e) { refused2 = e.toString().contains("AlreadyBeingCreatedException"); }
        check(refused2, "a second client cannot create a file another client is writing");
        check(((DistributedFileSystem) other).recoverLease(lp), "recoverLease by the second client");
        check(dfs.isFileClosed(lp), "the file is closed after recoverLease");
        boolean lost = false;
        try { held.close(); }
        catch (java.io.IOException e) { lost = e.toString().contains("LeaseExpiredException"); }
        check(lost, "the first writer has lost its lease (LeaseExpiredException on close)");
        other.close();

        check(fs.delete(new Path("/java"), true), "recursive delete");
        check(!fs.exists(new Path("/java")), "gone after delete");

        try (FSDataOutputStream out = fs.create(KEEP)) {
            out.write(keepData());
        }
        check(fs.getFileStatus(KEEP).getLen() == keepData().length, "wrote " + KEEP + " for the restart check");
        fs.close();
        System.out.println("ALL OK");
    }

    /**
     * Write 1.5 blocks, signal `ready` and wait for `go` while the script
     * kills a DataNode of the pipeline, then write 2 more blocks and close:
     * DataStreamer must rebuild the pipeline without it.
     */
    static void pipelineAcrossADatanodeFailure(FileSystem fs, String ready, String go) throws Exception {
        Path p = new Path("/pipeline/f.bin");
        byte[] data = new byte[7 << 19];
        new Random(9).nextBytes(data);
        try (FSDataOutputStream out = fs.create(p)) {
            out.write(data, 0, 3 << 19);
            Thread.sleep(1000);   // the packets reach the DataNodes and are acked
            Files.writeString(java.nio.file.Path.of(ready), "ready");
            while (!Files.exists(java.nio.file.Path.of(go))) Thread.sleep(100);
            out.write(data, 3 << 19, data.length - (3 << 19));
        }
        check(fs.getFileStatus(p).getLen() == data.length, "write completed across a DataNode failure");
        byte[] back = new byte[data.length];
        try (FSDataInputStream in = fs.open(p)) {
            in.readFully(back);
        }
        check(Arrays.equals(back, data), "content is byte-identical after pipeline recovery");
        fs.close();
        System.out.println("ALL OK pipeline");
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
