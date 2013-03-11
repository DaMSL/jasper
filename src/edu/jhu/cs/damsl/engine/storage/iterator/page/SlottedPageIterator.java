package edu.jhu.cs.damsl.engine.storage.iterator.page;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.catalog.identifiers.tuple.SlottedTupleId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;

public class SlottedPageIterator extends
                PageIterator<SlottedTupleId, SlottedPageHeader, SlottedPage>
{
  protected int currentSlot, returnedSlot;
  protected SlottedTupleId start, end;

  public SlottedPageIterator(PageId id, SlottedPage p) {
    super(id, p);
    start = end = null;
    reset();
  }

  public SlottedPageIterator(PageId id, SlottedPage p,
                             SlottedTupleId start,
                             SlottedTupleId end)
  {
    super(id, p);
    this.start = start;
    this.end = end;
    reset();
  }

  public void reset() {
    returnedSlot = currentSlot = SlottedPageHeader.INVALID_SLOT;
    nextValidTuple();
  }

  protected boolean isValidTuple(int slot) {
    return currentPage.getHeader().isValidTuple(slot);
  }

  public boolean isCurrentValid() {
    return isValidTuple(currentSlot);
  }
  
  public boolean isReturnedValid() {
    return isValidTuple(returnedSlot);
  }

  protected void nextValidTuple() {
    SlottedPageHeader h = currentPage.getHeader();
    
    int nextSlot = currentSlot+1;
    if ( currentSlot == SlottedPageHeader.INVALID_SLOT && start != null ) {
      nextSlot = start.slot();
    } else if ( end != null && currentSlot == end.slot() ) {
      nextSlot = SlottedPageHeader.INVALID_SLOT;
    } else {
      while ( h.isValidSlot(nextSlot) && !h.isValidTuple(nextSlot) ) { 
        nextSlot += 1;
      }
    }
    
    if ( h.isValidSlot(nextSlot) ) { currentSlot = nextSlot; }
    else { currentSlot = SlottedPageHeader.INVALID_SLOT; }
    
    currentTupleId = currentSlot == SlottedPageHeader.INVALID_SLOT ? null :
      new SlottedTupleId(currentPageId, h.getSlotLength(currentSlot), currentSlot);
  }

  protected Tuple getTuple() {
    return currentPage.getTuple(currentTupleId);
  }
  
  protected void removeTuple() {
    short tupleSize = currentPage.getHeader().getSlotLength(returnedSlot);
    SlottedTupleId id = new SlottedTupleId(currentPageId, tupleSize, returnedSlot);
    currentPage.removeTuple(id);
  }
  
  protected void markReturned() { returnedSlot = currentSlot; }
  
  @Override
  public boolean hasNext() { return isCurrentValid(); }

  @Override
  public Tuple next() {
    Tuple r = null;
    if ( isCurrentValid() ) {
      r = getTuple();
      markReturned();
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
      returnedSlot = SlottedPageHeader.INVALID_SLOT;
    }
  }

}
