package edu.jhu.cs.damsl.engine.storage.iterator.page;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.ContiguousTupleId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;

public class ContiguousPageIterator
                extends PageIterator<ContiguousTupleId, PageHeader, ContiguousPage>
{
  protected short currentOffset, returnedOffset;
  protected ContiguousTupleId start, end;

  public ContiguousPageIterator(PageId id, ContiguousPage p) {
    super(id, p);
    start = end = null;
    reset();
  }

  public ContiguousPageIterator(PageId id, ContiguousPage p,
                                ContiguousTupleId start,
                                ContiguousTupleId end)
  {
    super(id, p);
    this.start = start;
    this.end = end;
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

  protected void nextValidTuple() {
    PageHeader hdr = currentPage.getHeader();
    
    if ( currentOffset == PageHeader.INVALID_OFFSET ) {
      currentOffset = start == null ? hdr.getDataOffset() : start.offset();
    } else {
      // Do not advance any further if we are at the last desired tuple.
      currentOffset = (end != null && currentOffset == end.offset())?
        PageHeader.INVALID_OFFSET : hdr.getNextTupleOffset(currentOffset);
    }
    
    currentTupleId = currentOffset == PageHeader.INVALID_OFFSET? null :
      new ContiguousTupleId(currentPage.getId(), hdr.getTupleSize(), currentOffset);
  }
  
  protected void markReturned(Tuple t) {
    returnedOffset = currentOffset;
  }
  
  protected Tuple getTuple() {
    return currentPage.getTuple(currentTupleId);
  }
  
  protected void removeTuple() { 
    short tupleSize = currentPage.getHeader().getTupleSize();
    ContiguousTupleId id =
      new ContiguousTupleId(currentPage.getId(), tupleSize, returnedOffset);
    currentPage.removeTuple(id);
  }
  
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
    // Currently, we cannot remove tuples via this iterator if it
    // has a desired endpoint. This would require updating the endpoint
    // on removals.
    if ( end != null ) { throw new UnsupportedOperationException(); }

    if ( isReturnedValid() ) {
      removeTuple();
      currentOffset = returnedOffset;
      returnedOffset = PageHeader.INVALID_OFFSET;
      currentTupleId =
        new ContiguousTupleId(currentPage.getId(),
          currentPage.getHeader().getTupleSize(), currentOffset);
    }
  }

}
