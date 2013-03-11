package edu.jhu.cs.damsl.engine.storage.index;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.catalog.identifiers.IndexId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.DbBufferPool;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.file.StorageFile;
import edu.jhu.cs.damsl.engine.storage.iterator.index.IndexIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.engine.transactions.TransactionAbortException;
import edu.jhu.cs.damsl.utils.hw2.HW2.*;

/**
 * Represents the logical interface of an index.
 */
@CS316Todo(methods = "isEmpty, containsEntry, containsKey, firstKey, lastKey, " +
                     "lowerBound, upperBound, get, getAll, put, putAll, remove, removeAll")

@CS416Todo(methods = "isEmpty, containsEntry, containsKey, firstKey, lastKey, " +
                     "lowerBound, upperBound, get, getAll, put, putAll, remove, removeAll")

public class Index<IdType extends TupleId>
{
  IndexId indexId;
  IndexFile<IdType> indexFile;

  DbBufferPool<IndexId, ContiguousTupleId,
               PageHeader, IndexPage<IdType>, IndexFile<IdType>> pool;

  public Index(DbBufferPool<IndexId, ContiguousTupleId, PageHeader,
                            IndexPage<IdType>, IndexFile<IdType>> p,
               IndexId id, IndexFile<IdType> f)
  {
    pool = p;
    indexId = id;
    indexFile = f;
  }

  public IndexId getId() { return indexId; }


  /**
   * Return the file backing this index.
   */
  public IndexFile<IdType> getFile() { return indexFile; }


  // Index API, based on a multimap.
  
  /**
   * Return whether or not the index is empty.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public boolean isEmpty() {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Determine if the index contains the given (key, record) association.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public boolean containsEntry(Tuple key, IdType record) {
    throw new UnsupportedOperationException();
  }
  
  /**
   * Determine if the given key is contained in the index.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public boolean containsKey(Tuple key) {
    throw new UnsupportedOperationException();
  }
    
  /**
   * Get the first key (in sorted order) in the index.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public Tuple firstKey() {
    throw new UnsupportedOperationException();
  }
    
  /**
   * Return the last key (in sorted order) in the index.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public Tuple lastKey() {
    throw new UnsupportedOperationException();
  }

  /** Returns the first leaf entry that matches the given key.
    */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  protected ContiguousTupleId lowerBound(Tuple key) {
    throw new UnsupportedOperationException();
  } 

  /** Returns the first leaf entry that is strictly greater
    * than the given key.
    */ 
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  protected ContiguousTupleId upperBound(Tuple key) {
    throw new UnsupportedOperationException();
  }

  /**
   * Get the first tuple corresponding to the given key
   * in the index, null if no such tuple exists.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public IdType get(Tuple key) {
    throw new UnsupportedOperationException();
  }
    
  /**
   * Get all the tuples corresponding to the given key
   * in the index, an empty collection if no such tuple exists.
   */
  @CS316Todo(exercise = 1)
  @CS416Todo(exercise = 1)
  public Collection<IdType> getAll(Tuple key) {
    throw new UnsupportedOperationException();
  }
    
  /**
   * Insert the given key into the index, pointing to the given
   * tuple in the base relation.
   */
  @CS316Todo(exercise = 2)
  @CS416Todo(exercise = 2)
  public boolean put(Tuple key, IdType id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Insert the given key into the index, once for each
   * occurrence in the base relation.
   */
  @CS316Todo(exercise = 2)
  @CS416Todo(exercise = 2)
  public boolean putAll(Tuple key, List<IdType> ids) {
    throw new UnsupportedOperationException();
  }
    
  /**
   * Remove a particular (key, occurrence) from the index.
   */
  @CS316Todo(exercise = 3)
  @CS416Todo(exercise = 3)
  public boolean remove(Tuple key, IdType id) {
    throw new UnsupportedOperationException();
  }

  /**
   * Remove all occurrences of the given key from the index.
   */
  @CS316Todo(exercise = 3)
  @CS416Todo(exercise = 3)
  public boolean removeAll(Tuple key) {
    throw new UnsupportedOperationException();
  }

  
  // Iterators.
  
  /**
   * Return an iterator to the sorted contents of the base
   * relation, using the index.
   */
  public IndexIterator<IdType> scan() {
    return indexFile.index_iterator();
  }

  /**
   * Return an iterator to the sorted contents of the base
   * relation starting from the given key, using the index.
   */
  public IndexIterator<IdType> seek(Tuple key)
    throws TransactionAbortException
  {
    ContiguousTupleId start = lowerBound(key);
    ContiguousTupleId end = upperBound(key);
    return indexFile.index_iterator(start, end);
  }

}
