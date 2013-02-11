package edu.jhu.cs.damsl.engine.storage;

import java.util.LinkedList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Catalog;
import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.catalog.specs.TableSpec;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.StorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.iterator.file.StorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.BaseStorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.buffered.BufferedSlottedFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.WrappedStorageIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.util.MultiplexedListIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.storage.page.factory.PageFactory;
import edu.jhu.cs.damsl.engine.transactions.TransactionAbortException;
import edu.jhu.cs.damsl.language.core.types.TypeException;

/**
  * A storage engine implementation comprising a buffer pool and a file manager.
  * This class provides a high-level API for other engine components to access
  * the records making up relations, through pages and files.
  * In addition to data access, this class provides methods to modify the database
  * implementation as the database schema is modified by the catalog.
  * That is, as relations are added and deleted from the database, the storage
  * engine instructs the file manager to add and delete the operating system
  * files containing relation data.
  */
public class StorageEngine<HeaderType extends PageHeader,
                           PageType   extends Page<HeaderType>,
                           FileType   extends StorageFile<HeaderType, PageType>>
{
  protected static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);

  // Handle to a catalog, which may be a shadow catalog.
  protected Catalog catalog;

  // Page factory.
  protected PageFactory<HeaderType, PageType> pageFactory;
  
  // Raw storage layer.
  protected DbBufferPool<HeaderType, PageType, FileType> pool;
  protected FileManager<HeaderType, PageType, FileType> fileMgr;
  
  public StorageEngine(Catalog c, 
                       StorageFileFactory<HeaderType, PageType, FileType> f)
  {
    catalog = c;
    pageFactory = f.getPageFactory();
    pool = new DbBufferPool<HeaderType, PageType, FileType>(this,
                  Defaults.defaultBufferPoolSize, Defaults.defaultBufferPoolUnit,
                  Defaults.defaultPageSize, Defaults.defaultPageUnit);
    
    fileMgr = new FileManager<HeaderType, PageType, FileType>(pool, f);
    initialize();
  }
  
  protected StorageEngine(StorageEngine<HeaderType, PageType, FileType> e) {
    catalog     = e.catalog;
    pageFactory = e.pageFactory;
    pool        = e.pool;
    fileMgr     = e.fileMgr;
  }

  protected StorageEngine(Catalog c, StorageEngine<HeaderType, PageType, FileType> s) {
    catalog     = c;
    pageFactory = s.pageFactory;
    pool        = s.pool;
    fileMgr     = s.fileMgr.getShadow();
  }
  
  // Create a double buffer, with local copies of the file and index manager
  // to support database catalog modifications.
  public StorageEngine<HeaderType, PageType, FileType> getShadow(Catalog shadowCatalog) {
    return new StorageEngine<HeaderType, PageType, FileType>(shadowCatalog, this);
  }
  
  public Catalog getCatalog() { return catalog; }
  public PageFactory<HeaderType, PageType> getPageFactory() { return pageFactory; }
  public DbBufferPool<HeaderType, PageType, FileType> getBufferPool() { return pool; }
  public FileManager<HeaderType, PageType, FileType> getFileManager() { return fileMgr; }

  // Storage engine initialization
  private void initialize() {
    for ( Map.Entry<String, TableSpec> e : catalog.getTables().entrySet() ) {
      try {
        fileMgr.loadRelation(e.getValue().getId(), e.getValue().getSchema());
      } catch (TypeException t) {
        logger.warn("skipping load of table {}", e.getKey());
      }
    }
  }

  // Base relations.
  public void addRelation(TransactionId txn, TableId rel, Schema sch) {
    fileMgr.addRelation(rel, sch);
  }

  public void loadRelation(TransactionId txn, TableId rel, Schema sch) {
    fileMgr.loadRelation(rel, sch);
  }

  public void removeRelation(TransactionId txn, TableId rel) {
    fileMgr.removeRelation(rel);
  }

  public void clearRelation(TransactionId txn, TableId rel) {
    fileMgr.clearRelation(rel);
  }
  
  StorageIterator bufferedIterators(TransactionId txn,
                                    Page.Permissions perm,
                                    LinkedList<FileType> files)
  {
    LinkedList<StorageIterator> iterators = new LinkedList<StorageIterator>();
    for (FileType f : files) {
      iterators.add(f.buffered_iterator(txn,perm));
    }

    return new WrappedStorageIterator(
        new MultiplexedListIterator<Tuple, StorageIterator>(iterators));
  }
  
  // Returns an iterator multiplexed over all storage files, with buffered
  // page access per storage file.
  public StorageIterator scanRelation(TransactionId txn,
                                      TableId rel,
                                      Page.Permissions perm)
  {
    return bufferedIterators(txn, perm, fileMgr.getFiles(rel));
  }
  

  // Pages
  /**
    * Retrieves the page corresponding to the given page id with the
    * access mode specified.
    * <p>
    * Transaction ids should be ignored for Homework 1.
    *
    * @param txn 
    * @param pid
    * @param perm
    */

  public PageType getPage(TransactionId txn, PageId pid, Page.Permissions perm) {
    return pool.getPage(pid);
  }

  public void releasePage(TransactionId txn, PageId pid) {
    pool.flushPage(pid);
  }

  // Records.

  /**
    * Inserts the given tuple into the relation.
    * <p>
    * Transaction ids should be ignored for Homework 1.
    *
    * @param txn 
    * @param rel
    * @param t
    */
  public void insertTuple(TransactionId txn, TableId rel, Tuple t) 
    throws TransactionAbortException
  {
    short requestedSize = (short) (t.isFixedLength()? t.getFixedLength() : t.size()); 
    PageId id = pool.getWriteablePage(rel, requestedSize);
    if ( id == null ) {
      logger.error("No valid page found when requesting writeable page");
      throw new TransactionAbortException();
    }
    
    // Write the tuple and flush the page.
    PageType p = getPage(txn, id, Page.Permissions.WRITE);
    if ( p == null || !p.putTuple(t, requestedSize) ) {
      logger.error("Invalid page, and tuple insertion");
      throw new TransactionAbortException();
    }
  }

  /**
    * Deletes the given tuple from the relation.
    * <p>
    * Transaction ids should be ignored for Homework 1.
    *
    * @param txn 
    * @param rel
    * @param t
    */
  public void deleteTuple(TransactionId txn, TableId rel, Tuple t)
    throws TransactionAbortException
  {
    StorageIterator relIt = scanRelation(txn, rel, Page.Permissions.WRITE);
    while ( relIt.hasNext() ) {
      Tuple rt = relIt.next();
      if ( rt.equals(t) ) { relIt.remove(); }
    }
  }

  public String toString() {
    String r = "==== Buffer pool ====\n" + pool.toString() + "\n\n";
    r += "==== File manager ====\n" + fileMgr.toString();
    return r;
  }

}
