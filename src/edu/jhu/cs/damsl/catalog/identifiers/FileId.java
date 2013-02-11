package edu.jhu.cs.damsl.catalog.identifiers;

import java.io.File;
import java.io.Serializable;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Addressable;
import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.language.core.types.StringType;

public class FileId implements Addressable, Serializable {

  protected File filePath;
  protected int pageSize;
  protected int numPages;
  protected long capacity;

  public FileId(String fileName) {
    this(new File(fileName).getAbsoluteFile(), 0, 0, Defaults.getDefaultFileSize());
  }

  public FileId(File f) { 
    this(f.getAbsolutePath(), 0, 0, Defaults.getDefaultFileSize());
  }

  public FileId(String fileName, int pageSize, int numPages, long capacity) {
    this(new File(fileName).getAbsoluteFile(), pageSize, numPages, capacity);
  }

  public FileId(File f, int pageSz, int nPages, long cap) {
    filePath = f;
    pageSize = pageSz;
    numPages = nPages;
    capacity = cap;
  }

  @Override
  public int getAddress() { return filePath.hashCode(); }

  @Override
  public String getAddressString() { return filePath.getAbsolutePath(); }

  public File getFile() { return filePath; }

  public int pageSize() { return pageSize; }

  public void setPageSize(int p) { pageSize = p; }

  public int numPages() { return numPages; }

  public void setNumPages(int n) { numPages = n; }

  public long capacity() { return capacity; }

  public void setCapacity(long cap) { capacity = cap; }


  // Buffer I/O
  // TODO: use an external file map of file names to integers, and only
  // write out integer file ids.
  public static FileId read(ChannelBuffer buf) {
    FileId r = null;
    int length = buf.readInt();
    if ( length > 0 ) {
      String fName = buf.readSlice(length).toString(Defaults.defaultCharset);
      int pageSize = buf.readInt();
      int numPages = buf.readInt();
      long capacity = buf.readLong();
      r = new FileId(fName, pageSize, numPages, capacity);
    }
    return r;
  }
  
  public void write(ChannelBuffer buf) {
    String path = filePath.getAbsolutePath();
    buf.writeInt(StringType.getStringByteLength(path));
    buf.writeBytes(path.getBytes(Defaults.defaultCharset));
    buf.writeInt(pageSize);
    buf.writeInt(numPages);
    buf.writeLong(capacity);
  }
  
  public static void writeEmpty(ChannelBuffer buf) {
    buf.writeInt(INVALID_FILE);
  }
  
  public short size() {
    return (short) (EMPTY_SIZE +
          StringType.getStringByteLength(filePath.getAbsolutePath()));
  }

  public String toString() { return getAddressString()+"("+getAddress()+")"; }

  public static Integer INVALID_FILE = -1;
  public static Integer EMPTY_SIZE = ((Integer.SIZE*3+Long.SIZE)>>3);

}
