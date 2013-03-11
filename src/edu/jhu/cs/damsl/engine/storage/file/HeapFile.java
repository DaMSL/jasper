package edu.jhu.cs.damsl.engine.storage.file;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.FileId.FileKind;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.page.HeaderFactory;

public abstract class HeapFile<
                          IdType extends TupleId,
                          HeaderType extends PageHeader,
                          PageType extends Page<IdType, HeaderType>>
                      implements StorageFile<IdType, HeaderType, PageType>
{
  protected static final Logger logger = LoggerFactory.getLogger(HeapFile.class);
  FileId fileId;
  RandomAccessFile file;
  
  TableId relation;
  Schema tupleSchema;
  
  public HeapFile(FileKind k, Integer pageSize, Long capacity)
      throws FileNotFoundException
  {
    this(k, pageSize, capacity, null);
  }

  public HeapFile(FileKind k, Integer pageSize, Long capacity, Schema sch)
      throws FileNotFoundException
  {
    this(k, pageSize, capacity, sch, null);
  }

  public HeapFile(FileKind k, Integer pageSz, Long cap, Schema sch, TableId rel)
      throws FileNotFoundException
  {
    initialize(new FileId(k, pageSz, 0, cap), sch, rel);
  }

  public HeapFile(FileId fId, Schema sch, TableId rel)
      throws FileNotFoundException
  {
    fileId = fId;
    initialize(fId, sch, rel);
  }

  private void initialize(FileId fId, Schema sch, TableId rel)
    throws FileNotFoundException
  {
    fileId = fId;
    file = new RandomAccessFile(fileId.file().getAbsolutePath(), "rwd");
    tupleSchema = sch;
    relation = rel;
  }

  public String name() { return fileId.getAddressString(); }
  
  public FileId fileId() { return fileId; }

  public boolean isSorted() { return false; }

  public Schema getSchema() { return tupleSchema; }

  public TableId getRelation() { return relation; }

  public void setRelation(TableId rel) { relation = rel; }

  public int pageSize() { return fileId.pageSize(); }

  public void setPageSize(int p) { fileId.setPageSize(p); }
  
  public int numPages() { return fileId.numPages(); }
  
  public void setNumPages(int n) { fileId.setNumPages(n); }

  long actual_size() {
    long r = -1;
    try { r = file.length(); } catch (IOException e) {
      logger.error("invalid length for {}", fileId.getAddressString());
      e.printStackTrace();
    }
    return r;
  }
  
  public long size() { return numPages()*pageSize(); }
  public long capacity() { return fileId.capacity(); }
  public long remaining() { return capacity() - size(); }

  public void extend(int pageCount) {
    long requestedSize = (numPages()+pageCount)*pageSize();
    if ( requestedSize < capacity() ) {
      try {
        file.setLength(requestedSize);
        setNumPages(numPages()+pageCount);
        logger.info("extending file to {} pages (size in bytes {})", numPages(), size());
      } catch (IOException e) {
        logger.error("could not extend file by {} pages", pageCount);
      }
    } else {
      logger.error("requested extend of {} pages beyond capacity {}", pageCount, capacity());
    }
  }

  public void shrink(int pageCount) {
    if ( pageCount < numPages() ) {
      try {
        file.setLength((numPages()-pageCount) * pageSize());
        setNumPages(numPages()-pageCount);
        logger.info("shrinking file to {} pages (size in bytes {})", numPages(), size());
      } catch (IOException e) {
        logger.error("could not shrink file by {} pages", pageCount);
      }
    } else {
      logger.error("requested shrink of {} pages beyond current {} pages", pageCount, numPages());
    }
  }

  // Initialize an in-memory page with any available schema information from the file.
  public void initializePage(PageType buf) {
    buf.setHeader(buf.getHeaderFactory().getHeader(tupleSchema, buf, (byte) 0));
  }

  // Reads the requested page id into the given buffer.
  // Returns the number of bytes successfully read from the heap file.
  public int readPage(PageType buf, PageId id) {
    int read = 0;
    if ( buf.capacity() == pageSize()
          && id.fileId().equals(fileId) && id.pageNum() < numPages() ) 
    {
      int offset = pageSize() * id.pageNum();
      try {
        file.seek(offset);
        read = buf.setBytes(0, file.getChannel(), pageSize());
        buf.setId(id);
        buf.markReaderIndex(); buf.markWriterIndex();
        buf.setIndex(0, buf.capacity());
        buf.readHeader(); // Refresh the in-mem header.
        buf.resetReaderIndex(); buf.resetWriterIndex();
      } catch (IOException e) {
        logger.error("file {} failed to read page {}", fileId, id.pageNum());
        e.printStackTrace();
      }
    } else {
      logger.error("file id eq: {}, pnum lt: {}", id.fileId().equals(fileId), id.pageNum() < numPages());
      logger.error(
          "file read mismatch (page:{} vs file:{})", id.fileId(), fileId);      
    }
    return read;
  }

  // Returns the number of bytes written from the page to the heap file.
  public int writePage(PageType p) {
    int written = 0;
    PageId pId = p.getId();

    // Set the page id for in-memory pages being written to this file.
    if ( pId.fileId() == null ) {
      pId = new PageId(fileId, pId.pageNum());
      p.setId(pId);
    }
    
    if ( p.capacity() == pageSize() && pId.fileId().equals(fileId) ) {
      long offset = pageSize() * pId.pageNum();
      if ( offset+pageSize() < capacity() ) {
        if ( pId.pageNum() >= numPages() ) { extend(pId.pageNum()-numPages()+1); }
        try {
          file.seek(offset);
          p.markReaderIndex(); p.markWriterIndex();
          p.setIndex(0, 0);
          p.writeHeader(); // Refresh the disk header.
          written = p.getBytes(0, file.getChannel(), pageSize());
          p.resetReaderIndex(); p.resetWriterIndex();
        } catch (IOException e) {
          logger.error("file {} failed to write page {}", fileId, pId.pageNum());
          e.printStackTrace();
        }
      } else {
        logger.error("attempt to write file {} beyond capacity", fileId);
      }
    } else {
      logger.error(
          "file write mismatch (page:{} vs file:{})", pId.fileId(), fileId);
    }
    return written;
  }

  public abstract HeaderFactory<HeaderType> getHeaderFactory();

  protected HeaderType readCurrentPageHeader() throws IOException {
    return getHeaderFactory().readHeaderDirect(file);
  }

  public HeaderType readPageHeader(PageId id) {
    HeaderType r = null;
    if ( id.fileId().equals(fileId) && id.pageNum() < numPages() ) {
      int offset = pageSize() * id.pageNum();
      try {
        file.seek(offset);
        r = readCurrentPageHeader();
      } catch (IOException e) {
        logger.error("file {} failed to read page {}", fileId, id.pageNum());
        e.printStackTrace();
      }
    }
    return r;
  }

  public String toString() {
    return " pages: " + Integer.toString(numPages()) +
           " size: " + Long.toString(size()) +
           " cap: " + Long.toString(capacity()) +
           " actual: " + Long.toString(actual_size());
  }

}
