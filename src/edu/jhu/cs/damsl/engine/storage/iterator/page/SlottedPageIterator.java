package edu.jhu.cs.damsl.engine.storage.iterator.page;

import edu.jhu.cs.damsl.catalog.identifiers.PageId;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;

public class SlottedPageIterator
                extends PageIterator<SlottedPageHeader, SlottedPage>
{
  protected int currentSlot, returnedSlot;

  public SlottedPageIterator(PageId id, SlottedPage p) {
    super(id, p);
    reset();
  }

  public void reset() {
    returnedSlot = currentSlot = SlottedPageHeader.INVALID_SLOT;
    nextValidTuple();
  }

  boolean isValidTuple(int slot) {
    return currentPage.getHeader().isValidTuple(slot);
  }

  public boolean isCurrentValid() {
    return isValidTuple(currentSlot);
  }
  
  public boolean isReturnedValid() {
    return isValidTuple(returnedSlot);
  }

  void nextValidTuple() {
    SlottedPageHeader h = currentPage.getHeader();
    int nextSlot = currentSlot+1;
    while ( h.isValidSlot(nextSlot) && !h.isValidTuple(nextSlot) ) { 
      nextSlot += 1;
    }
    if ( h.isValidSlot(nextSlot) ) { currentSlot = nextSlot; }
    else { currentSlot = SlottedPageHeader.INVALID_SLOT; }
  }

  Tuple getTuple() {
    return currentPage.getTuple(currentSlot);
  }
  
  void removeTuple() {
    currentPage.removeTuple(returnedSlot);
  }
  
  void markReturned() { returnedSlot = currentSlot; }
  
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
    if ( isReturnedValid() ) {
      removeTuple();
      returnedSlot = SlottedPageHeader.INVALID_SLOT;
    }
  }

}
