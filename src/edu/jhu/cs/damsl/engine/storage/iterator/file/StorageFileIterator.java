package edu.jhu.cs.damsl.engine.storage.iterator.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.accessor.PageFileAccessor;
import edu.jhu.cs.damsl.engine.storage.iterator.page.StorageIterator;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public interface StorageFileIterator<
                      IdType extends TupleId,
          						HeaderType extends PageHeader,
          						PageType extends Page<IdType, HeaderType>>
          					extends StorageIterator
{
  public PageId nextPageId();
  public void nextValidTuple();
  public void markReturned();

  public boolean hasNext();
  public Tuple next();
  public void remove();
  public void reset();

  public PageFileAccessor<IdType, HeaderType, PageType> getAccessor();
}
