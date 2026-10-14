import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Stock Hadoop client against a Kerberized Pillar (realm PILLAR.TEST):
 *   KerberosInterop <hdfs-uri> kerberos <keytab dir>   users log in from keytabs
 *   KerberosInterop <hdfs-uri> simple                  must be refused
 *   KerberosInterop <hdfs-uri> unprotected <keytab dir>  alice, asking for less
 *                                                      protection than the server's
 * hadoop.rpc.protection is $RPC_PROTECTION (default authentication).
 */
public class KerberosInterop {
    static void check(boolean ok, String what) {
        if (!ok) throw new AssertionError(what);
        System.out.println("ok    " + what);
    }

    static Configuration conf(String uri, String auth) {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", uri);
        conf.set("hadoop.security.authentication", auth);
        String protection = System.getenv("RPC_PROTECTION");
        conf.set("hadoop.rpc.protection", protection == null || protection.isEmpty() ? "authentication" : protection);
        conf.set("dfs.namenode.kerberos.principal", "nn/_HOST@PILLAR.TEST");
        conf.set("dfs.datanode.kerberos.principal", "dn/_HOST@PILLAR.TEST");
        conf.set("dfs.replication", "3");
        conf.setLong("dfs.blocksize", 1L << 20);
        return conf;
    }

    public static void main(String[] args) throws Exception {
        String uri = args[0];
        if (args[1].equals("simple")) {
            Configuration conf = conf(uri, "simple");
            UserGroupInformation.setConfiguration(conf);
            String err = "";
            try (FileSystem fs = FileSystem.newInstance(conf)) {
                fs.listStatus(new Path("/"));
            } catch (Exception e) {
                err = e.toString();
            }
            check(err.contains("SIMPLE authentication is not enabled"), "a SIMPLE client is refused (" + err.split("\n")[0] + ")");
            System.out.println("ALL OK simple");
            return;
        }

        String dir = args[2];
        if (args[1].equals("unprotected")) {
            Configuration conf = conf(uri, "kerberos");
            conf.set("hadoop.rpc.protection", "authentication");
            UserGroupInformation.setConfiguration(conf);
            UserGroupInformation.loginUserFromKeytab("alice@PILLAR.TEST", dir + "/alice.keytab");
            String err = "";
            try (FileSystem fs = FileSystem.newInstance(conf)) {
                fs.listStatus(new Path("/"));
            } catch (Exception e) {
                err = e.toString();
                for (Throwable c = e.getCause(); c != null; c = c.getCause()) err += " / " + c;
            }
            check(err.contains("No common protection layer"),
                  "a client without " + System.getenv("RPC_PROTECTION") + " is refused (" + err.split("\n")[0] + ")");
            System.out.println("ALL OK unprotected");
            return;
        }
        Configuration conf = conf(uri, "kerberos");
        UserGroupInformation.setConfiguration(conf);

        // Permissions are enforced: hdfs (the superuser) opens / to users.
        UserGroupInformation hdfs = UserGroupInformation.loginUserFromKeytabAndReturnUGI("hdfs@PILLAR.TEST", dir + "/hdfs.keytab");
        hdfs.doAs((PrivilegedExceptionAction<Void>) () -> {
            try (FileSystem su = FileSystem.newInstance(conf)) {
                su.setPermission(new Path("/"), new org.apache.hadoop.fs.permission.FsPermission((short) 01777));
            }
            return null;
        });

        UserGroupInformation.loginUserFromKeytab("alice@PILLAR.TEST", dir + "/alice.keytab");
        FileSystem fs = FileSystem.get(conf);

        fs.mkdirs(new Path("/secure"));
        // bob writes here below; the authenticated user is what counts.
        fs.setPermission(new Path("/secure"), new org.apache.hadoop.fs.permission.FsPermission((short) 01777));
        byte[] data = new byte[(3 << 19) + 99];
        new Random(3).nextBytes(data);
        Path f = new Path("/secure/alice.bin");
        try (FSDataOutputStream out = fs.create(f)) { out.write(data); }
        byte[] back;
        try (FSDataInputStream in = fs.open(f)) { back = in.readAllBytes(); }
        check(Arrays.equals(back, data), "alice (Kerberos) writes and reads a multi-block file");
        check(fs.getFileStatus(f).getOwner().equals("alice"), "the file is owned by the Kerberos user, alice");

        // A file still open: the reader asks a DataNode of the last block
        // how much of it is visible, authenticating with the block's access
        // token (SASL DIGEST-MD5 on the DataNode's IPC port).
        Path open = new Path("/secure/open.bin");
        int flushed = (1 << 20) + 4321;
        try (FSDataOutputStream out = fs.create(open)) {
            out.write(data, 0, flushed);
            out.hflush();
            byte[] seen;
            try (FileSystem reader = FileSystem.newInstance(conf); FSDataInputStream in = reader.open(open)) {
                seen = in.readAllBytes();
            }
            check(Arrays.equals(seen, Arrays.copyOf(data, flushed)),
                  "another client reads the hflushed part of an open file (" + seen.length + " bytes)");
        }

        // Another user.
        UserGroupInformation bob = UserGroupInformation.loginUserFromKeytabAndReturnUGI("bob@PILLAR.TEST", dir + "/bob.keytab");
        String bobOwner = bob.doAs((PrivilegedExceptionAction<String>) () -> {
            try (FileSystem bfs = FileSystem.newInstance(conf)) {
                Path b = new Path("/secure/bob.txt");
                try (FSDataOutputStream out = bfs.create(b)) { out.write("bob".getBytes()); }
                return bfs.getFileStatus(b).getOwner();
            }
        });
        check(bobOwner.equals("bob"), "bob's file is owned by bob");

        // Permissions hold for authenticated users: alice's 600 file is hers.
        fs.setPermission(f, new org.apache.hadoop.fs.permission.FsPermission((short) 0600));
        String denied = bob.doAs((PrivilegedExceptionAction<String>) () -> {
            try (FileSystem bfs = FileSystem.newInstance(conf); FSDataInputStream in = bfs.open(f)) {
                in.read();
                return "";
            } catch (org.apache.hadoop.security.AccessControlException e) {
                return e.getMessage().split("\n")[0];
            }
        });
        check(denied.contains("user=bob, access=READ"), "bob cannot read alice's 600 file (" + denied + ")");

        // alice cannot act as bob: she is no proxy user.
        UserGroupInformation proxy = UserGroupInformation.createProxyUser("bob", UserGroupInformation.getLoginUser());
        String err = proxy.doAs((PrivilegedExceptionAction<String>) () -> {
            try (FileSystem pfs = FileSystem.newInstance(conf)) {
                pfs.listStatus(new Path("/"));
                return "";
            } catch (Exception e) {
                return e.toString();
            }
        });
        check(err.contains("User: alice@PILLAR.TEST is not allowed to impersonate bob"), "alice impersonating bob is refused");

        // oozie (Kerberos) may act as staff members (bob), not as alice.
        UserGroupInformation oozie = UserGroupInformation.loginUserFromKeytabAndReturnUGI("oozie@PILLAR.TEST", dir + "/oozie.keytab");
        Path viaOozie = new Path("/secure/oozie-for-bob.txt");
        String owner = UserGroupInformation.createProxyUser("bob", oozie).doAs((PrivilegedExceptionAction<String>) () -> {
            try (FileSystem pfs = FileSystem.newInstance(conf)) {
                try (FSDataOutputStream out = pfs.create(viaOozie)) { out.write("for bob".getBytes()); }
                return pfs.getFileStatus(viaOozie).getOwner();
            }
        });
        check(owner.equals("bob"), "oozie (Kerberos), a proxy user, writes a file as bob (owner " + owner + ")");
        err = UserGroupInformation.createProxyUser("alice", oozie).doAs((PrivilegedExceptionAction<String>) () -> {
            try (FileSystem pfs = FileSystem.newInstance(conf)) {
                pfs.listStatus(new Path("/"));
                return "";
            } catch (Exception e) {
                return e.toString();
            }
        });
        check(err.contains("User: oozie@PILLAR.TEST is not allowed to impersonate alice"), "oozie acting as alice (not in staff) is refused");

        fs.delete(new Path("/secure"), true);
        System.out.println("ALL OK kerberos");
    }
}
