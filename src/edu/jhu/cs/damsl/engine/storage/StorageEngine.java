package edu.jhu.cs.damsl.engine.storage;

import java.io.FileNotFoundException;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Catalog;
import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.TransactionId;
import edu.jhu.cs.damsl.catalog.specs.TableSpec;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.index.Index;
import edu.jhu.cs.damsl.engine.storage.index.IndexManager;
import edu.jhu.cs.damsl.engine.storage.iterator.file.StorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.BaseStorageFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.file.buffered.BufferedSlottedFileIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.WrappedStorageIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.util.MultiplexedListIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.transactions.TransactionAbortException;
import edu.jhu.cs.damsl.language.core.types.TypeException;
import edu.jhu.cs.damsl.factory.file.StorageFileFactory;
import edu.jhu.cs.damsl.factory.page.PageFactory;

// A proxy to storage layer components.
public class StorageEngine<IdType     extends TupleId,
                           HeaderType extends PageHeader,
                           PageType   extends Page<IdType, HeaderType>,
                           FileType   extends StorageFile<IdType, HeaderType, PageType>>
{
  protected static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);

  // Handle to a catalog, which may be a shadow catalog.
  protected Catalog catalog;

  // Page factory.
  protected PageFactory<IdType, HeaderType, PageType> pageFactory;
  
  // Raw storage layer.
  protected FileManager<IdType, HeaderType, PageType, FileType> fileMgr;
  protected DbBufferPool<TableId, IdType, HeaderType, PageType, FileType> pool;
  
  public StorageEngine(Catalog c, 
                       StorageFileFactory<IdType, HeaderType, PageType, FileType> f)
  {
    catalog = c;
    pageFactory = f.getPageFactory();
    fileMgr = new FileManager<IdType, HeaderType, PageType, FileType>(f);
    pool = new DbBufferPool<TableId, IdType, HeaderType, PageType, FileType>(
                  pageFactory, fileMgr,
                  Defaults.defaultBufferPoolSize, Defaults.defaultBufferPoolUnit,
                  Defaults.defaultPageSize, Defaults.defaultPageUnit);
    
    initialize();
  }
  
  protected StorageEngine(StorageEngine<IdType, HeaderType, PageType, FileType> e) {
    catalog     = e.catalog;
    pageFactory = e.pageFactory;
    fileMgr     = e.fileMgr;
    pool        = e.pool;
  }

  protected StorageEngine(Catalog c, StorageEngine<IdType, HeaderType, PageType, FileType> s) {
    catalog     = c;
    pageFactory = s.pageFactory;
    fileMgr     = s.fileMgr.getShadow();
    pool        = s.pool;
  }
  
  // Create a double buffer, with local copies of the file and index manager
  // to support database catalog modifications.
  public StorageEngine<IdType, HeaderType, PageType, FileType> getShadow(Catalog shadowCatalog) {
    return new StorageEngine<IdType, HeaderType, PageType, FileType>(shadowCatalog, this);
  }
  
  public Catalog getCatalog() { return catalog; }
  
  public PageFactory<IdType, HeaderType, PageType>
  getPageFactory() { return pageFactory; }
  
  public FileManager<IdType, HeaderType, PageType, FileType>
  getFileManager() { return fileMgr; }
  
  public IndexManager<IdType, HeaderType, PageType, FileType>
  getIndexManager() { return fileMgr.getIndexManager(); }

  public DbBufferPool<TableId, IdType, HeaderType, PageType, FileType>
  getBufferPool() { return pool; }

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

  // Tuple DML methods.
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
    IdType tId = p == null? null : p.putTuple(t);
    if ( p == null || tId == null ) {
      logger.error("Invalid page, and tuple insertion");
      throw new TransactionAbortException();
    }

    // Maintain any indexes.
    if ( tId != null ) {
      List<IndexId> indexes = catalog.getIndexes(rel);
      List<Index<IdType>> indexImpls = fileMgr.getIndexes(indexes);
      for (Index<IdType> idx : indexImpls) {
        // Add the key and its corresponding tuple id to the index.
        Tuple key = getIndexKey(t, rel, idx);
        idx.put(key, tId);
      }      
    }

    debugInserted(id);
  }

  // Delete a specific tuple from a relation.
  public void deleteTuple(TransactionId txn, TableId rel, IdType old)
    throws TransactionAbortException
  {
    checkTupleId(rel, old);

    // If the tuple's page is not in the buffer pool, fetch it.
    PageType p = old.pageId() == null ?
      null : getPage(txn, old.pageId(), Page.Permissions.WRITE);

    if ( p == null ) {
      logger.error("Invalid page during tuple deletion");
      throw new TransactionAbortException();      
    }

    // Retrieve the tuple to construct its key.
    Tuple t = p.getTuple(old);

    // Maintain any indexes.
    List<IndexId> indexes = catalog.getIndexes(rel);
    List<Index<IdType>> indexImpls = fileMgr.getIndexes(indexes);
    for (Index<IdType> idx : indexImpls) {
      // Remove the key and its corresponding tuple id from the index.
      Tuple key = getIndexKey(t, rel, idx);
      idx.remove(key, old);
    }

    // Delete the tuple in memory and mark the page dirty.
    p.removeTuple(old);
  }

  // Deletes all tuples in the relation matching the given tuple.
  public void deleteTuple(TransactionId txn, TableId rel, Tuple t)
    throws TransactionAbortException
  {
    // Maintain any indexes.
    List<IndexId> indexes = catalog.getIndexes(rel);
    List<Index<IdType>> indexImpls = fileMgr.getIndexes(indexes);
    for (Index<IdType> idx : indexImpls) {
      // Remove the key and its corresponding tuple id from the index.
      Tuple key = getIndexKey(t, rel, idx);
      idx.removeAll(key);
    }

    StorageIterator relIt = scanRelation(txn, rel, Page.Permissions.WRITE);
    while ( relIt.hasNext() ) {
      Tuple rt = relIt.next();
      if ( rt.equals(t) ) {
        relIt.remove();
      }
    }
  }

  // TODO: finish
  // Update a specific tuple in a relation.
  public void updateTuple(TransactionId txn, TableId rel, IdType old, Tuple update)
    throws TransactionAbortException
  {
    checkTupleId(rel, old);
    
    // If the tuple's page is not in the buffer pool, fetch it.
    PageType p = old.pageId() == null ?
      null : getPage(txn, old.pageId(), Page.Permissions.WRITE);

    if ( p == null ) {
      logger.error("Invalid page during tuple deletion");
      throw new TransactionAbortException();      
    }

    // Update the tuple in memory.
    // TODO
  }

  // TODO: finish
  // Updates all tuples in the relation matching the given tuple.
  public void updateTuple(TransactionId txn, TableId rel, Tuple old, Tuple update)
    throws TransactionAbortException
  {
    // TODO: index scan if possible rather than sequential scan.
    StorageIterator relIt = scanRelation(txn, rel, Page.Permissions.WRITE);
    while ( relIt.hasNext() ) {
      Tuple rt = relIt.next();
      if ( rt.equals(old) ) { /* TODO update */ }
    }

  }

  // Helper methods for Tuple DML.
  static int tuplesInserted = 0;
  
  private void debugInserted(PageId id) {
    tuplesInserted += 1;
    if ( (tuplesInserted % 10000) == 0 ) {
      logger.info("writing to page {}", id.getAddressString());
    }
  }

  // Check if the tuple is present in a valid relation file.
  protected void checkTupleId(TableId relId, IdType tId)
    throws TransactionAbortException
  {
    List<FileId> relFiles = relId.files();
    FileId fId = tId.pageId() == null ? null : tId.pageId().fileId();
    
    if ( fId == null ||
          !(getFileManager().isRelationFile(fId) && relFiles.contains(fId)) )
    {
      logger.error("Invalid relation file during tuple deletion");
      throw new TransactionAbortException();
    }
  }

  // Create an index key tuple from a relation tuple
  protected Tuple getIndexKey(Tuple t, TableId rel, Index idx)
    throws TransactionAbortException
  {
    // Create a tuple containing only the key fields.
    // This projects the tuple by interpreting its fields as objects,
    // and then only selecting those fields matching a key attribute.
    // We assume typechecking has occurred by this point to ensure the
    // index key attributes are present and valid in the relation schema.
    Schema relSchema = null;
    try { relSchema = catalog.getTableByName(rel.name()).getSchema(); }
    catch (TypeException e) { throw new TransactionAbortException(); }

    List<Object> fields = t.interpretTuple(relSchema);
    
    Schema keySchema = idx.getId().schema();
    List<Object> keyFields = new LinkedList<Object>();
    for ( String fieldName : keySchema.getFields() ) {
      Integer pos = relSchema.getFieldPosition(fieldName); 
      if ( pos == null ) { throw new TransactionAbortException(); }
      keyFields.add(fields.get(pos));
    }
    
    // Return a heap-allocated tuple for the key. This tuple will be
    // garbage-collected when the tuple is written to the index page.
    return Tuple.schemaTuple(keySchema, keyFields);
  }

  // Iterators, for tuples and tuple identifiers.

  // Internal method to return a multiplexed proxy iterator.
  protected StorageIterator
  bufferedIterators(TransactionId txn, Page.Permissions perm,
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
  public StorageIterator
  scanRelation(TransactionId txn, TableId rel, Page.Permissions perm)
  {
    return bufferedIterators(txn, perm, fileMgr.getFiles(rel));
  }
  
  // Returns an iterator over tuple identifiers for the given index.
  public IndexIterator<IdType>
  scanIndex(TransactionId txn, IndexId idx, Page.Permissions perm) {
    return fileMgr.scanIndex(idx);
  }
  
  // Returns an iterator over tuple identifiers that match the given key,
  // for a specific index.
  public IndexIterator<IdType>
  seekIndex(TransactionId txn, IndexId idx, Tuple key, Page.Permissions perm)
    throws TransactionAbortException
  {
    return fileMgr.seekIndex(idx, key);
  }


  // Pages
  public PageType getPage(TransactionId txn, PageId pid, Page.Permissions perm) {
    return pool.getPage(pid);
  }

  public void releasePage(TransactionId txn, PageId pid) {
    pool.flushPage(pid);
  }


  // Base relations.
  // These are proxy methods for the file manager's relation management methods.
  public void addRelation(TransactionId txn, TableId rel, Schema sch) 
    throws TransactionAbortException
  {
    try { fileMgr.addRelation(rel, sch); 
    } catch (FileNotFoundException e) { throw new TransactionAbortException(); }
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


  // Indexes.
  // These are proxy methods for the file manager's index management methods.
  public void addIndex(TransactionId txn, TableId rel, IndexId idx)
    throws TransactionAbortException
  { 
    try { fileMgr.addIndex(rel, idx);
    } catch (FileNotFoundException e) { throw new TransactionAbortException(); }
  }
  
  public void removeIndex(TransactionId txn, IndexId id) {
    fileMgr.removeIndex(id);
  }

  public void loadIndex(TransactionId txn, IndexId id, FileId file) {
    fileMgr.loadIndex(id, file);
  }

  // Debugging helpers.

  public String toString() {
    String r = "==== Buffer pool ====\n" + pool.toString() + "\n\n";
    r += "==== File manager ====\n" + fileMgr.toString();
    return r;
  }

}
