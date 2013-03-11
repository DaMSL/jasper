package edu.jhu.cs.damsl.catalog.identifiers;

import java.io.Serializable;

import org.jboss.netty.buffer.ChannelBuffer;

import edu.jhu.cs.damsl.catalog.Addressable;

public class PageId implements Addressable, Serializable {
  
  FileId fileId;
  Integer pageNum;
  
  // Main-memory pages do not have an associated file id.
  public PageId(int pageNum) {
    fileId = null;
    this.pageNum = pageNum;
  }
  
  public PageId(FileId fileId, int pageNum) {
    this.fileId = fileId;
    this.pageNum = pageNum;
  }
  
  @Override
  public boolean equals(Object o) {
    if ( o == null || !(o instanceof PageId) ) { return false; }
    if ( o == this ) { return true; }
    PageId other = (PageId) o;
    return other.fileId.equals(fileId) && other.pageNum == pageNum;
  }

  @Override
  public int hashCode() { 
    return fileId.getAddress() + Integer.valueOf(pageNum).hashCode();
  }

  @Override
  public int getAddress() { return hashCode(); }

  @Override
  public String getAddressString() {
    return fileId.getAddressString()+":P"+pageNum();
  }

  // Primitive accessors.  
  public FileId fileId() { return fileId; }
  public int pageNum() { return pageNum; }

  // Buffer I/O
  public static PageId read(ChannelBuffer buf) {
    FileId f = FileId.read(buf);
    int pagenum = buf.readInt();
    return new PageId(f, pagenum);
  }
  
  public void write(ChannelBuffer buf) {
    if ( fileId != null ) { fileId.write(buf); }
    else { FileId.writeEmpty(buf); }
    buf.writeInt(pageNum);
  }
  
  public short size() {
    return (short) ((fileId == null?
              FileId.EMPTY_SIZE : fileId.size())+(Integer.SIZE>>3));
  }

}
