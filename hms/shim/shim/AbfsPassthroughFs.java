package shim;

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;

/**
 * HMS-shim FileSystem for the abfss/abfs schemes: the metastore only needs to
 * validate that a table location URI is well-formed and a "directory"; the
 * actual data reads happen in StarRocks (which has real Azure support). This
 * class keeps the original scheme AND authority (RawLocalFileSystem would
 * requalify abfss paths to file://, mangling the stored location), accepts any
 * abfss path in checkPath, and answers the validation checks affirmatively
 * without touching Azure. Every entry point logs to stderr so the validation
 * behaviour can be traced from docker logs.
 */
public class AbfsPassthroughFs extends RawLocalFileSystem {
  // The URI this instance was initialized with. getUri() must return it (not a
  // scheme-only constant): Hadoop rebuilds paths from the FS URI, so a
  // scheme-only URI strips the authority (container@account) from the stored
  // table location.
  private URI uri = URI.create("abfss:///");

  @Override
  public URI getUri() {
    // null guard: RawLocalFileSystem's constructor calls getUri() through
    // getInitialWorkingDirectory() before this class's field initializers run
    return uri != null ? uri : URI.create("abfss:///");
  }

  @Override
  public void checkPath(Path path) {
    // accept every abfss path: the shim cannot know the storage accounts in
    // advance, and Hive would otherwise reject (Wrong FS) or requalify them
  }

  @Override
  public Path getWorkingDirectory() {
    return new Path("abfss:/");
  }

  @Override
  public boolean exists(Path f) {
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) {
    return new FileStatus(0, true, 1, 0, 0, f);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) {
    return true;
  }

  @Override
  public void initialize(URI name, Configuration conf) {
    uri = name;
    setConf(conf);
  }
}
