package edu.jhu.cs.damsl.catalog.identifiers;

import java.io.File;
import java.io.Serializable;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Addressable;
import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.language.core.types.StringType;

public class FileId implements Addressable, Serializable {

  public enum FileKind { Relation, Index, Temporary };

  private static Integer counter = 0;
  protected int fileId;
  protected FileKind fileKind;
  
  protected int pageSize;
  protected int numPages;
  protected long capacity;

  // We now use an implicit file name, that can always be
  // derived from the fileId integer.
  protected File filePath;

  public FileId(FileKind k) {
    this(k, 0, 0, Defaults.getDefaultFileSize());
  }

  public FileId(FileKind k, int pageSize, int numPages, long capacity) {
    this(counter++, k, pageSize, numPages, capacity);
  }

  protected FileId(int fId, FileKind k, int pageSz, int nPages, long cap) {
    fileId   = fId;
    fileKind = k;
    pageSize = pageSz;
    numPages = nPages;
    capacity = cap;
    filePath = new File(getFileNamePrefix(fileKind)+Integer.toString(fileId));
  }

  public String toString() { return getAddressString()+"("+getAddress()+")"; }

  @Override
  public boolean equals(Object o) {
    if ( o == null || !(o instanceof FileId) ) { return false; }
    return o == this || (((FileId) o).fileId == fileId);
  }

  @Override
  public int hashCode() { return Integer.valueOf(fileId).hashCode(); }

  @Override
  public int getAddress() { return hashCode(); }

  @Override
  public String getAddressString() { return filePath.getAbsolutePath(); }

  public File file() { return filePath; }

  public FileKind fileKind() { return fileKind; }

  public int pageSize() { return pageSize; }

  public void setPageSize(int p) { pageSize = p; }

  public int numPages() { return numPages; }

  public void setNumPages(int n) { numPages = n; }

  public long capacity() { return capacity; }

  public void setCapacity(long cap) { capacity = cap; }

  // File naming helpers.
  public static String getFileNamePrefix(FileKind k) {
    String r = Defaults.defaultDbFilePrefix+"-";
    switch (k) {
      case Index: r += "idx"; break;
      case Temporary: r += "tmp"; break;
      case Relation:
      default:
        r += "rel"; break;
    }
    return r;
  }

  // Buffer I/O
  public static byte packFileKind(FileKind k) {
    byte r = (byte) 0x0;
    switch (k) {
      case Index: r = (byte) 0x4; break;
      case Temporary: r = (byte) 0x2; break;
      case Relation:
      default:
        r = (byte) 0x1; break;
    }
    return r;
  }

  public static FileKind unpackFileKind(byte b) {
    FileKind r = null;
    if ( b == (byte) 0x4 )      { r = FileKind.Index; }
    else if ( b == (byte) 0x2 ) { r = FileKind.Temporary; }
    else if ( b == (byte) 0x1 ) { r = FileKind.Relation; }
    return r;
  }

  public static FileId read(ChannelBuffer buf) {
    FileId r = null;
    byte done = buf.readByte();
    if ( done == VALID_ID ) {
      FileKind fileKind = unpackFileKind(buf.readByte());
      int fileId        = buf.readInt();
      int pageSize      = buf.readInt();
      int numPages      = buf.readInt();
      long capacity     = buf.readLong();
      r = new FileId(fileId, fileKind, pageSize, numPages, capacity);
    }
    return r;
  }

  public void write(ChannelBuffer buf) {
    buf.writeByte(VALID_ID);
    buf.writeByte(packFileKind(fileKind));
    buf.writeInt(fileId);
    buf.writeInt(pageSize);
    buf.writeInt(numPages);
    buf.writeLong(capacity);    
  }

  public static void writeEmpty(ChannelBuffer buf) {
    buf.writeByte(EMPTY_ID);
  }

  public short size() {
    return Integer.valueOf((Byte.SIZE+Integer.SIZE*3+Long.SIZE)>>3).shortValue();
  }

  public static final byte EMPTY_ID = (byte) 0x1;
  public static final byte VALID_ID = (byte) 0x0;

  public static final short EMPTY_SIZE = Integer.valueOf(Byte.SIZE>>3).shortValue();

}
