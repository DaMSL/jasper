package edu.jhu.cs.damsl.engine.storage;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.Addressable;
import edu.jhu.cs.damsl.catalog.Durable;
import edu.jhu.cs.damsl.catalog.Defaults;
import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.FileId;
import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TableId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.file.factory.StorageFileFactory;
import edu.jhu.cs.damsl.engine.storage.iterator.file.header.StorageFileHeaderIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.page.WrappedStorageIterator;
import edu.jhu.cs.damsl.engine.storage.iterator.util.MultiplexedListIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.utils.LRUCache;
import edu.jhu.cs.damsl.utils.hw1.HW1.*;

/**
 * Manages a collection of files belonging to the database.
 *
 * A FileManager provides an interface to the files which hold the data in the database. Files can
 * be added directly, or as part of a relation. Files in turn contain pages, the reading and writing
 * of which are managed by a buffer pool.
 *
 * Note this class is not thread-safe. Callers must implement table-level
 * locking to ensure consistency with multi-threaded relation manipulation.
 */
@CS316Todo
@CS416Todo
public class FileManager<HeaderType extends PageHeader,
                         PageType   extends Page<HeaderType>,
                         FileType   extends StorageFile<HeaderType, PageType>>
{
  protected static final Logger logger = LoggerFactory.getLogger(FileManager.class);
  DbBufferPool<HeaderType, PageType, FileType> pool;

  // File factory
  StorageFileFactory<HeaderType, PageType, FileType> fileFactory;

  // Relations.
  HashMap<FileId, FileType> dbFiles;

  public FileManager(DbBufferPool<HeaderType, PageType, FileType> p,
                     StorageFileFactory<HeaderType, PageType, FileType> f)
  {
    pool = p;
    fileFactory = f;
    dbFiles = new HashMap<FileId, FileType>();
  }

  @SuppressWarnings("unchecked")
  private FileManager(FileManager<HeaderType, PageType, FileType> f)
  {
    pool = f.pool;
    fileFactory = f.fileFactory;
    dbFiles = (HashMap<FileId, FileType>) f.dbFiles.clone();
  }

  public FileManager<HeaderType, PageType, FileType> getShadow() {
    return new FileManager<HeaderType, PageType, FileType>(this); 
  }

  // Reporting.
  public String toString() {
    String r = "";
    for ( Map.Entry<FileId, FileType> e : dbFiles.entrySet() ) {
      r += e.getKey().toString() + ": " + e.getValue().toString() + "\n";
    }
    return r;
  }

  // File management helpers.
  <IdType extends Addressable>
  String getDbFileName(IdType id, List<FileId> files) {
    return "jdbf"+Integer.toString(id.getAddress())+files.size();
  }


  <IdType extends Durable>
  FileId addFile(HashMap<FileId, FileType> files,
                 IdType id, Integer pageSize, Long fileCapacity, Schema sch)
  {
    FileId r = null;
    // Add only to relation/file map if this is a new relation. The header
    // cache will be created lazily on the first caching attempt.
    List<FileId> fList = id.getFiles();
    String fileName = getDbFileName(id, fList);

    try {
      FileType f = fileFactory.getFile(fileName, sch);
      r = f.fileId();
      fList.add(r);
      files.put(r, f);
    } catch (FileNotFoundException e) { e.printStackTrace(); }
    
    return r;
  }
  
  FileId addFile(TableId rel, Integer pageSize, Long fileCapacity, Schema sch) {
    FileId r = addFile(dbFiles, rel, pageSize, fileCapacity, sch);
    dbFiles.get(r).setRelation(rel);
    return r;
  }

  void loadFile(HashMap<FileId, FileType> files, TableId rel, FileId id, Schema sch)
  {
    String filePath = id.getFile().getAbsolutePath();
    try {
      FileType f = fileFactory.getFile(id, sch, rel);
      files.put(f.fileId(), f);
    } catch (FileNotFoundException e) { e.printStackTrace(); }
  }

  <IdType extends Durable>
  void removeFile(HashMap<FileId, FileType> files, IdType id, FileId f) {
    List<FileId> rf = id.getFiles();
    if ( rf != null && rf.contains(f) ) {
      File fPath = f.getFile();
      FileType s = files.get(f);
      if ( s != null && fPath.exists() && fPath.canWrite() ) {
        rf.remove(f);
        files.remove(f);
        fPath.delete();
      }
    } else {
      logger.error("expected table {} to use file {}", id, f);
    }
  }
  
  void removeFile(TableId rel, FileId f) {
    removeFile(dbFiles, rel, f);
  }


  // Database file modifiers. 
  public void addRelation(TableId rel, Schema sch) {
    List<FileId> files = rel.getFiles();
    if ( files == null || files.isEmpty() ) {
      addFile(rel, pool.getPageSize(), Defaults.getDefaultFileSize(), sch);
    } else {
      logger.warn("found duplicate db files for relation {}", files);
      rel = null;
    }
  }

  public void loadRelation(TableId rel, Schema sch) {
    List<FileId> files = rel.getFiles();
    if ( files != null ) {
      for (FileId id : files) { loadFile(dbFiles, rel, id, sch); }
    }
  }

  public void clearRelation(TableId rel) {
    List<FileId> files = rel.getFiles();
    if ( files == null || files.isEmpty() ) {
      logger.warn("no db files found for relation {}", files);
    } else {
      for (FileId fId : files) { removeFile(rel, fId); }
    }
  }

  public void removeRelation(TableId rel) {
    clearRelation(rel);
  }

  // Table accessors.
  
  // Returns a multiplexed iterator that directly uses a page allocator (i.e.,
  // the buffer pool), to bypass any page caching.
  public StorageIterator scanRelation(TableId rel) {
    LinkedList<StorageIterator> iterators = new LinkedList<StorageIterator>();
    for ( FileType f : getFiles(rel) ) { 
      iterators.add(f.iterator()); 
    }

    return new WrappedStorageIterator(
        new MultiplexedListIterator<Tuple, StorageIterator>(iterators));
  }

  // Files.  
  public LinkedList<FileType> getFiles(TableId rel) {
    LinkedList<FileType> r = null;
    List<FileId> files = rel.getFiles();
    if ( files != null && files.size() > 0 ) {
      r = new LinkedList<FileType>();
      for (FileId fId : files) {
        FileType f = dbFiles.get(fId);
        if ( f != null ) r.add(f);
      }
    }
    return r;
  }
  
  public boolean isRelationFile(FileId fileId) {
    return dbFiles.containsKey(fileId);
  }
  
  public TableId getFileRelation(FileId fileId) {
    FileType f = dbFiles.get(fileId);
    return ( f == null ? null : f.getRelation());
  }
  

  // Page accessors.

  /**
    * Reads the on-disk page corresponding to the given page id into
    * the given buffer. The page id must be a valid on-disk page,
    * that is, its file id must correspond to a database file tracked
    * by this file manager.
    *
    * @param buf an in-memory page supplied by the buffer pool which
    *            is filled by the page from disk
    * @param id the identifier of the page being accessed
    * @return the buffer if the page is successfully read, otherwise null
    */
  @CS316Todo
  @CS416Todo
  public PageType readPage(PageType buf, PageId id) {
    return null;
  }

  /**
    * Writes the given page to the on-disk location specified by the page's
    * identifier. This page id must contain a valid file id corresponding
    * to a database file tracked by this file manager.
    *
    * @param page a page to be written to disk
    * @return the number of bytes written by this operation
    */
  @CS316Todo
  @CS416Todo
  public int writePage(PageType page) {
    return 0;
  }

  /**
    * Searches the database files for the given relation, looking for
    * a page with the requested free space. This method also accepts
    * a list of pages cached by the buffer pool to exclude from consideration
    * since they should already have been inspected at this point.
    *
    * @param rel the relation to which we want to write
    * @param requestedSpace the amount of space requested
    * @param cached a collection of page ids for cached pages that have been checked
    * @return the page id of a suitable page in the files for the relation
    */
  @CS316Todo
  @CS416Todo
  public PageId getWriteablePage(
      TableId rel, short requestedSpace, Collection<PageId> cached)
  {
    return null;
  }

}
