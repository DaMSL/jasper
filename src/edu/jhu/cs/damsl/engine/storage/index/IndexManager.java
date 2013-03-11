package edu.jhu.cs.damsl.engine.storage.index;

import java.io.FileNotFoundException;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.FileId.FileKind;
import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.accessor.FileAccessor;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.StorageFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.index.IndexFile;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.factory.file.StorageFileFactory;
import edu.jhu.cs.damsl.factory.index.IndexFileFactory;
import edu.jhu.cs.damsl.factory.page.IndexPageFactory;
import edu.jhu.cs.damsl.factory.tuple.TupleIdFactory;
import edu.jhu.cs.damsl.utils.hw2.HW2.*;

@CS416Todo(methods = "loadIndex")
public class IndexManager<IdType      extends TupleId,
                          HeaderType  extends PageHeader,
                          PageType    extends Page<IdType, HeaderType>,
                          FileType    extends StorageFile<IdType, HeaderType, PageType>>
                implements FileAccessor<IndexId, ContiguousTupleId, PageHeader, IndexPage<IdType>>
{  
  // We use a separate page pool for index pages.
  DbBufferPool<IndexId, ContiguousTupleId,
               PageHeader, IndexPage<IdType>, IndexFile<IdType>> idxPagePool;

  // A set of indexes. 
  LinkedHashMap<IndexId, Index<IdType>> indexes;
  HashMap<FileId, IndexFile<IdType>> indexFiles;

  // Factories.
  IndexPageFactory<IdType> pageFactory;
  TupleIdFactory<IdType> tupleIdFactory;
  IndexFileFactory<IdType> indexFileFactory;
  
  public IndexManager(StorageFileFactory<IdType, HeaderType, PageType, FileType> f)
  {
    pageFactory = new IndexPageFactory<IdType>();
    tupleIdFactory = f.getTupleIdFactory();
    indexFileFactory = f.getIndexFileFactory();

    idxPagePool =
      new DbBufferPool<IndexId, ContiguousTupleId,
                       PageHeader, IndexPage<IdType>, IndexFile<IdType>>
          (
            pageFactory, this,
            Defaults.defaultIndexBufferPoolSize, Defaults.defaultIndexBufferPoolUnit,
            Defaults.defaultPageSize, Defaults.defaultPageUnit);

    indexes = new LinkedHashMap<IndexId, Index<IdType>>();
    indexFiles = new HashMap<FileId, IndexFile<IdType>>();
  }
  
  @SuppressWarnings("unchecked")
  private IndexManager(IndexManager i)
  {
    idxPagePool = i.idxPagePool;
    indexes = (LinkedHashMap<IndexId, Index<IdType>>) i.indexes.clone();
    indexFiles = (HashMap<FileId, IndexFile<IdType>>) i.indexFiles.clone();
    pageFactory = i.pageFactory;
    tupleIdFactory = i.tupleIdFactory;
    indexFileFactory = i.indexFileFactory;
  }
  
  public IndexManager getShadow() {
    return new IndexManager(this);
  }

  // Basic accessors.
  public DbBufferPool<IndexId, ContiguousTupleId,
                      PageHeader, IndexPage<IdType>, IndexFile<IdType>>
  getIndexBufferPool() { return idxPagePool; }

  // Index management methods.

  // Adds an index with a key over the fields given by the schema to a relation.
  // The relation is comprised of the given files for index construction on an
  // existing relation.
  public void addIndex(TableId rel, IndexId idxId, LinkedList<FileType> files)
    throws FileNotFoundException
  {
    Schema keySchema = idxId.schema();
    IndexFile<IdType> idxFile =
      indexFileFactory.getIndexFile(
        idxPagePool, tupleIdFactory, idxId, keySchema, FileKind.Index);
    
    Index<IdType> idx = new Index<IdType>(idxPagePool, idxId, idxFile);
    
    indexes.put(idxId, idx);
    if ( idx.getFile() != null ) { 
      indexFiles.put(idx.getFile().fileId(), idx.getFile());
    }

    // Bulk load all existing files into the index.      
    if ( files != null ) {
      for (FileType f : files) { loadIndex(idxId, f); }
    }
  }

  // Retrieves the index implementation for a given index id.
  public Index<IdType> getIndex(IndexId id) {
    return indexes.get(id);
  }

  // Retrieves the index implementations for a list of index ids. 
  public LinkedList<Index<IdType>> getIndexes(List<IndexId> indexes)
  {
    LinkedList<Index<IdType>> r = new LinkedList<Index<IdType>>();
    for (IndexId id : indexes) { r.add(getIndex(id)); }
    return r;
  }

  // Removes the index corresponding to the given index id.
  public void removeIndex(IndexId id)
  {
    Index<IdType> idx = indexes.get(id);
    if ( idx != null && idx.getFile() != null ) {
      indexFiles.remove(idx.getFile().fileId());
    } 
    indexes.remove(id);
  }

  // Index accessors.
  public boolean isIndexFile(FileId fileId) { 
    return indexFiles.containsKey(fileId);
  }

  public IndexFile<IdType> getIndexFile(FileId fileId) {
    IndexFile<IdType> f = isIndexFile(fileId) ? indexFiles.get(fileId) : null;
    return f;
  }

  /* File accessor implementation.
     This allows the index manager to interact with a buffer pool,
     supplying methods for the buffer pool to use to read and write
     index pages, as well as find a writeable page (i.e., the
     getWriteablePage) method.

     For now, we can simply copy the file manager's methods. In the future,
     this should be refactored across the file and index manager.
  */

  public IndexPage<IdType> readPage(IndexPage<IdType> buf, PageId id) {
    IndexPage<IdType> r = null;
    FileId fId = id.fileId();
    if ( fId != null ) {
      boolean idxFile = isIndexFile(fId);
      IndexFile<IdType> f = idxFile ? indexFiles.get(fId) : null;
      if ( f != null ) {
        r = buf;
        int read = f.readPage(r, id);
        if ( read != r.capacity() ) r = null;
      }
    }
    return r;
  }

  public int writePage(IndexPage<IdType> page) {
    int r = 0;
    FileId fId = page.getId().fileId();
    if ( fId != null ) {
      boolean idxFile = isIndexFile(fId);
      IndexFile<IdType> f = idxFile ? indexFiles.get(fId) : null;
      if ( f != null ) { r = f.writePage(page); }
    }
    return r;
  }

  public PageId getWriteablePage(IndexId id, short requestedSpace,
                                 Collection<PageId> cached)
  {
    PageId r = null;    

    // Search through disk page headers.
    // This operation could be extremely expensive and should be a rare event.
    IndexFile<IdType> f = indexes.get(id).getFile();
    
    StorageFileHeaderIterator<ContiguousTupleId, PageHeader, IndexPage<IdType>> hIt =
      f.header_iterator();
    
    while ( r == null && hIt.hasNext() ) {
      PageId pId = hIt.getPageId();
      PageHeader h = hIt.next();
      if ( h != null && ( cached == null || !cached.contains(pId) )
          && h.isSpaceAvailable(requestedSpace) )
      { r = pId; }
    }
    
    return r;
  }

  public PageId extendFile(IndexId id, IndexPage<IdType> buf, short requestedSpace)
  {
    PageId r = null;
    IndexFile<IdType> f = indexes.get(id).getFile();
    if ( f != null && f.remaining() > requestedSpace ) {
      r = new PageId(f.fileId(), f.numPages());
      f.extend(1);
      f.initializePage(buf);
      buf.setId(r);
    }
    
    return r;
  }


  /**
    * Bulk load an index from a specific file.
    *
    * For this assignment, you may assume that the file is already sorted.
    */
  @CS416Todo(exercise = 4)
  public void loadIndex(IndexId id, FileType file) {
    throw new UnsupportedOperationException();
  }

}
