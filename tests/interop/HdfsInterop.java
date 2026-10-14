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
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Stock Hadoop DFSClient against Pillar: the code path `hadoop fs` uses.
 * Runs as alice; the NameNode enforces permissions (superuser hdfs, groups
 * alice=staff and bob=staff), and oozie may act as bob from 127.0.0.1.
 */
public class HdfsInterop {
    interface Action { void run(FileSystem fs) throws Exception; }

    /** Run `a` as `user` (SIMPLE: the client names its user). */
    static void as(String user, Configuration conf, Action a) throws Exception {
        UserGroupInformation.createRemoteUser(user).doAs((java.security.PrivilegedExceptionAction<Void>) () -> {
            try (FileSystem fs = FileSystem.newInstance(conf)) { a.run(fs); }
            return null;
        });
    }

    /** Whether `a`, as `user`, is refused with an AccessControlException. */
    static String refused(String user, Configuration conf, Action a) throws Exception {
        String[] why = {null};
        as(user, conf, fs -> {
            try { a.run(fs); } catch (AccessControlException e) { why[0] = e.getMessage().split("\n")[0]; }
        });
        return why[0];
    }

    static void denied(String user, Configuration conf, String what, Action a) throws Exception {
        String why = refused(user, conf, a);
        check(why != null, what + " is refused (" + why + ")");
    }

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

        // The root is the superuser's; it opens it to everyone, like /tmp.
        denied("alice", conf, "alice creating a directory in hdfs's 755 /", u -> u.mkdirs(new Path("/java")));
        as("hdfs", conf, su -> su.setPermission(new Path("/"), new FsPermission((short) 01777)));
        FsPermission root = fs.getFileStatus(new Path("/")).getPermission();
        check(root.toShort() == 01777 && root.getStickyBit(), "the superuser makes / world-writable and sticky (" + root + ")");

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
        fs.setTimes(m, 1_000_000L, 2_000_000L);
        check(fs.setReplication(m, (short) 2), "setReplication");
        fs.access(m, FsAction.READ);
        // Giving a file away takes the superuser.
        denied("alice", conf, "alice giving her file to bob", u -> u.setOwner(m, "bob", null));
        denied("alice", conf, "alice moving her file to a group she is not in", u -> u.setOwner(m, null, "eng"));
        fs.setOwner(m, null, "staff");
        check(fs.getFileStatus(m).getGroup().equals("staff"), "alice moves her file to her group, staff");
        as("hdfs", conf, su -> su.setOwner(m, "bob", "eng"));
        FileStatus ms = fs.getFileStatus(m);
        check(ms.getPermission().toShort() == 0640 && ms.getOwner().equals("bob") && ms.getGroup().equals("eng")
              && ms.getModificationTime() == 1_000_000L && ms.getAccessTime() == 2_000_000L && ms.getReplication() == 2,
              "setPermission/setOwner/setTimes/setReplication are visible");
        // bob's file now, rw-r----- for eng: nothing for alice.
        boolean noRead = false;
        try { fs.access(m, FsAction.READ); } catch (AccessControlException e) { noRead = true; }
        check(noRead, "access(READ) on bob's 640 file is refused for alice");
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

        // hflush: another client reads what the writer flushed while the file
        // is still open (the last block's DataNodes report its visible
        // length); hsync(UPDATE_LENGTH) makes the NameNode's length catch up.
        Path hp = new Path("/java/hflush.bin");
        byte[] hd = new byte[(3 << 19) + 777];
        new Random(11).nextBytes(hd);
        int flushed = (1 << 20) + 300000;   // ends inside a checksum chunk
        FileSystem reader = FileSystem.newInstance(fs.getUri(), conf);
        try (FSDataOutputStream hout = fs.create(hp)) {
            hout.write(hd, 0, flushed);
            hout.hflush();
            byte[] seen;
            try (FSDataInputStream in = reader.open(hp)) { seen = in.readAllBytes(); }
            check(seen.length == flushed && Arrays.equals(seen, Arrays.copyOf(hd, flushed)),
                  "a reader sees hflushed data of an open file (" + seen.length + " bytes)");
            hout.write(hd, flushed, hd.length - flushed);
            ((HdfsDataOutputStream) hout).hsync(EnumSet.of(HdfsDataOutputStream.SyncFlag.UPDATE_LENGTH));
            check(reader.getFileStatus(hp).getLen() == hd.length, "hsync(UPDATE_LENGTH) updates the length the NameNode reports");
            try (FSDataInputStream in = reader.open(hp)) { seen = in.readAllBytes(); }
            check(Arrays.equals(seen, hd), "a reader sees hsynced data, past the flushed partial chunk");
        }
        try (FSDataInputStream in = reader.open(hp)) {
            check(Arrays.equals(in.readAllBytes(), hd), "the closed file has every byte");
        }
        reader.close();

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

        // Block recovery: a writer hflushes 1.5 blocks and walks away; another
        // client's recoverLease starts recovery on the DataNodes (false),
        // then reports the file closed (true) with every hflushed byte.
        Path rp = new Path("/java/recover.bin");
        byte[] rd = new byte[3 << 19];
        new Random(13).nextBytes(rd);
        FSDataOutputStream abandoned = fs.create(rp);
        abandoned.write(rd);
        abandoned.hflush();
        FileSystem rescuer = FileSystem.newInstance(fs.getUri(), conf);
        DistributedFileSystem rdfs = (DistributedFileSystem) rescuer;
        boolean first = rdfs.recoverLease(rp);
        boolean closed = first;
        for (int i = 0; !closed && i < 60; i++) {
            Thread.sleep(500);
            closed = rdfs.recoverLease(rp);
        }
        check(!first && closed, "recoverLease runs block recovery (false), then the file is closed (true)");
        byte[] recovered;
        try (FSDataInputStream in = rescuer.open(rp)) { recovered = in.readAllBytes(); }
        check(Arrays.equals(recovered, rd), "block recovery kept every hflushed byte (" + recovered.length + ")");
        try { abandoned.close(); } catch (java.io.IOException expected) { }
        rescuer.close();

        permissionChecks(conf, fs);
        // bob's renamed.bin is in alice's /java: hers to delete with it.
        check(fs.delete(new Path("/java"), true), "recursive delete");
        check(!fs.exists(new Path("/java")), "gone after delete");

        try (FSDataOutputStream out = fs.create(KEEP)) {
            out.write(keepData());
        }
        check(fs.getFileStatus(KEEP).getLen() == keepData().length, "wrote " + KEEP + " for the restart check");
        fs.close();
        System.out.println("ALL OK");
    }

    /** What bob, in alice's group, may and may not do to alice's files. */
    static void permissionChecks(Configuration conf, FileSystem fs) throws Exception {
        Path home = new Path("/java/home");
        fs.mkdirs(home);
        fs.setPermission(home, new FsPermission((short) 0750));
        Path notes = new Path(home, "notes.txt");
        try (FSDataOutputStream out = fs.create(notes)) { out.write("alice's notes\n".getBytes()); }
        fs.setPermission(notes, new FsPermission((short) 0640));
        FileStatus st = fs.getFileStatus(home);
        // / is hdfs:supergroup, so /java and everything alice makes in it
        // are in supergroup too (alice is not in it).
        check(st.getGroup().equals("supergroup"), "a new directory takes its parent's group (" + st.getGroup() + ")");
        fs.setOwner(home, null, "staff");
        fs.setOwner(notes, null, "staff");

        // Group r-x / r--: bob lists and reads, but cannot write.
        as("bob", conf, b -> {
            check(b.listStatus(home).length == 1, "bob (staff) lists alice's 750 directory");
            try (FSDataInputStream in = b.open(notes)) {
                check(new String(in.readAllBytes()).equals("alice's notes\n"), "bob (staff) reads alice's 640 file");
            }
        });
        denied("bob", conf, "bob writing alice's 640 file", b -> b.append(notes).close());
        denied("bob", conf, "bob creating a file in alice's 750 directory", b -> b.create(new Path(home, "bob.txt")).close());
        denied("bob", conf, "bob deleting alice's file", b -> b.delete(notes, false));
        denied("bob", conf, "bob renaming alice's file", b -> b.rename(notes, new Path("/java/stolen.txt")));
        denied("bob", conf, "bob changing the mode of alice's file", b -> b.setPermission(notes, new FsPermission((short) 0666)));
        denied("bob", conf, "bob changing the times of alice's file", b -> b.setTimes(notes, 1L, 1L));

        // Others (carol, in no group) cannot even traverse.
        String why = refused("carol", conf, c -> c.getFileStatus(notes));
        check(why != null && why.contains("access=EXECUTE"), "carol cannot traverse alice's 750 directory (" + why + ")");

        // The sticky / : bob may create there, but not remove alice's /java.
        as("bob", conf, b -> {
            check(b.mkdirs(new Path("/bob")), "bob creates /bob in the sticky, world-writable /");
            check(b.delete(new Path("/bob"), true), "bob deletes his own /bob");
        });
        why = refused("bob", conf, b -> b.delete(new Path("/java"), true));
        check(why != null && (why.contains("sticky bit") || why.contains("access=")), "bob deleting alice's /java is refused (" + why + ")");

        // Proxy users: oozie acts as bob (only bob); others act as nobody.
        UserGroupInformation oozie = UserGroupInformation.createRemoteUser("oozie");
        Path viaOozie = new Path("/oozie-for-bob.txt");
        String owner = UserGroupInformation.createProxyUser("bob", oozie).doAs((java.security.PrivilegedExceptionAction<String>) () -> {
            try (FileSystem p = FileSystem.newInstance(conf)) {
                p.create(viaOozie).close();
                String o = p.getFileStatus(viaOozie).getOwner();
                p.delete(viaOozie, false);
                return o;
            }
        });
        check(owner.equals("bob"), "oozie, a proxy user, creates a file as bob (owner " + owner + ")");
        for (String[] pair : new String[][] {{"oozie", "alice"}, {"eve", "bob"}}) {
            String err = UserGroupInformation.createProxyUser(pair[1], UserGroupInformation.createRemoteUser(pair[0]))
                .doAs((java.security.PrivilegedExceptionAction<String>) () -> {
                    try (FileSystem p = FileSystem.newInstance(conf)) {
                        p.listStatus(new Path("/"));
                        return "";
                    } catch (Exception e) {
                        return e.getMessage() == null ? e.toString() : e.getMessage().split("\n")[0];
                    }
                });
            check(err.contains("User: " + pair[0] + " is not allowed to impersonate " + pair[1]),
                  pair[0] + " acting as " + pair[1] + " is refused (" + err + ")");
        }

        // The superuser may do anything.
        as("hdfs", conf, su -> {
            try (FSDataInputStream in = su.open(notes)) { in.readAllBytes(); }
            su.setPermission(notes, new FsPermission((short) 0600));
        });
        check(fs.getFileStatus(notes).getPermission().toShort() == 0600, "the superuser reads and chmods any file");
        denied("bob", conf, "bob reading alice's file after it is made 600", b -> b.open(notes).close());
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
