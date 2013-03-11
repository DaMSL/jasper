package edu.jhu.cs.damsl.engine.storage.accessor;

import java.util.Collection;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.TupleId;
import edu.jhu.cs.damsl.engine.storage.page.Page;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public interface FileAccessor<EntityIdType,
                              TupleIdType extends TupleId,
                              HeaderType extends PageHeader,
                              PageType extends Page<TupleIdType, HeaderType>>
{
  public PageType readPage(PageType pageBuffer, PageId id);

  public int writePage(PageType page);

  public PageId getWriteablePage(EntityIdType id, short requestedSpace, Collection<PageId> cached);

  public PageId extendFile(EntityIdType id, PageType buf, short requestedSpace);
}