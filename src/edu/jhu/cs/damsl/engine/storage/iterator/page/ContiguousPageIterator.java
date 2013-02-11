package edu.jhu.cs.damsl.engine.storage.iterator.page;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class ContiguousPageIterator
                extends PageIterator<PageHeader, ContiguousPage>
{
  protected short currentOffset, returnedOffset;

  public ContiguousPageIterator(PageId id, ContiguousPage p) {
    super(id, p);
    reset();
  }

  public void reset() {
    returnedOffset = currentOffset = PageHeader.INVALID_OFFSET;
    nextValidTuple();
  }
  
  public boolean isCurrentValid() {
    return currentPage.isValidOffset(currentOffset);
  }

  public boolean isReturnedValid() {
    return currentPage.isValidOffset(returnedOffset);
  }

  void nextValidTuple() {
    PageHeader hdr = currentPage.getHeader();
    currentOffset = currentOffset <= 0?
        hdr.getDataOffset() : hdr.getNextTupleOffset(currentOffset);
  }
  
  void markReturned(Tuple t) {
    returnedOffset = currentOffset;
  }
  
  Tuple getTuple() { return currentPage.getTuple(currentOffset); }
  
  void removeTuple() { currentPage.removeTuple(returnedOffset); }
  
  @Override
  public boolean hasNext() { return isCurrentValid(); }
  
  @Override
  public Tuple next() {
    Tuple r = null;
    if ( isCurrentValid() ) {
      r = getTuple();
      markReturned(r);
      nextValidTuple();
    }
    return r;
  }

  @Override
  public void remove() {
    if ( isReturnedValid() ) {
      removeTuple();
      currentOffset = returnedOffset;
      returnedOffset = PageHeader.INVALID_OFFSET;
    }
  }

}
