package edu.jhu.cs.damsl.engine.storage.iterator.page;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPage;
import edu.jhu.cs.damsl.engine.storage.page.SlottedPageHeader;
import edu.jhu.cs.damsl.utils.SlottedPageTestUtils;

public class SlottedPageIteratorTest {

  private SlottedPageTestUtils ptUtils;
  private List<Tuple> tuples;
  private List<SlottedPage> testPages;

  @Before
  public void setUp() {
    ptUtils = new SlottedPageTestUtils();
    tuples = ptUtils.getTuples();
    testPages = ptUtils.generateSlottedPages(tuples);
  }

  void checkInitialIterator(SlottedPage p, SlottedPageIterator it) {
    // The iterator should be initialized to the first valid tuple,
    // without having returned a tuple yet.
    assertTrue ( it.getPage() == p
                  && it.getId() == p.getId()
                  && it.isCurrentValid() && !it.isReturnedValid() );
  }
  
  void checkFinalIterator(Tuple t, SlottedPageIterator it) {
    assertTrue( !it.hasNext() && !it.isCurrentValid()
        && (t != null) == (it.isReturnedValid()) );
  }
  
  Tuple checkTupleRetrieval(SlottedPage p, SlottedPageIterator it, int slotIdx) {
    Tuple tSlot = p.getTuple(slotIdx);
    assertTrue( it.hasNext() && it.isCurrentValid() );
    Tuple r = it.next();
    assertTrue( r != null && tSlot.equals(r) && it.isReturnedValid() );
    return r;
  }
  
  void checkTupleRemoval(SlottedPageHeader hdr, SlottedPageIterator it, int slotIdx) {
    it.remove();
    assertTrue( !it.isReturnedValid() && !hdr.isValidTuple(slotIdx) );
  }
  
  @Test
  public void resetTest() {
    for (SlottedPage p : testPages) {
      Tuple t = null;
      SlottedPageIterator it = p.iterator();
      checkInitialIterator(p,it);
      
      while ( it.hasNext() ) {
        t = it.next();
        assertTrue ( t != null && it.isReturnedValid() );
      }
      
      // Ensure iterator is expired.
      checkFinalIterator(t, it);
      
      // Reset, and ensure iterator is at its initial state.
      it.reset();
      checkInitialIterator(p,it);
    }
  }

  @Test
  public void isCurrentValidTest() {
    for (SlottedPage p : testPages) {
      SlottedPageIterator it = p.iterator();
      checkInitialIterator(p,it);
      
      while ( it.hasNext() ) {
        assertTrue ( it.isCurrentValid() );
        Tuple t = it.next();
        assertTrue ( t != null );
      }
      
      assertTrue( !it.isCurrentValid() );
    }
  }
  
  @Test
  public void isReturnedValidTest() {
    for (SlottedPage p : testPages) {
      Tuple t = null;
      SlottedPageIterator it = p.iterator();
      checkInitialIterator(p,it);
      
      while ( it.hasNext() ) {
        t = it.next();
        assertTrue ( t != null && it.isReturnedValid() );
      }
      
      // Ensure the final state of the tuple matches the return status
      // of the iterator.
      checkFinalIterator(t, it);
    }    
  }
  
  @Test
  public void hasNextAndNextTest() {
    for (SlottedPage p : testPages) {
      Tuple t = null;
      SlottedPageHeader hdr = p.getHeader();
      SlottedPageIterator it = p.iterator();
      checkInitialIterator(p,it);

      int numSlots = hdr.getNumSlots();
      
      // Ensure that we test against at least one tuple.
      assertTrue( numSlots > 0 );
      
      // Ensure that every tuple indicated by a slot is returned by the iterator.
      for (int i = 0; i < numSlots; ++i) {
        if ( hdr.isValidTuple(i) ) {
          t = checkTupleRetrieval(p, it, i);
        }
      }
      
      checkFinalIterator(t, it);
    }
  }

  @Test
  public void removeAllTest() {
    for (SlottedPage p : testPages) {
      SlottedPageHeader hdr = p.getHeader();
      SlottedPageIterator it = p.iterator();
      checkInitialIterator(p,it);
      int numSlots = hdr.getNumSlots();
      
      // Ensure that we test against at least one tuple.
      assertTrue( numSlots > 0 );
      
      for (int i = 0; i < numSlots; ++i) {
        // Verify tuple retrieval via iterator.
        if ( hdr.isValidTuple(i) ) {
          checkTupleRetrieval(p, it, i);
          
          // Ensure that iterator has an invalid return state, and that the
          // backing slot in the page no longer contains a valid tuple.
          checkTupleRemoval(hdr, it, i);
        }
      }

      // Ensure a valid final iterator state.
      checkFinalIterator(null, it);

      // Ensure the number of slots has not changed after removals.
      assertTrue ( hdr.getNumSlots() == numSlots );
      
      // Ensure that every slot no longer contains a valid tuple.
      for (int i = 0; i < numSlots; ++i) {
        assertTrue ( !hdr.isValidTuple(i) );
      }
    }    
  }

  @Test
  public void randomRemoveTest() {
    for (SlottedPage p : testPages) {

      LinkedList<Integer> deletedSlots = new LinkedList<Integer>();

      Tuple t = null;
      SlottedPageHeader hdr = p.getHeader();
      SlottedPageIterator it = p.iterator();
      checkInitialIterator(p,it);
      int numSlots = hdr.getNumSlots();
      
      // Ensure that we test against at least one tuple.
      assertTrue( numSlots > 0 );
      
      for (int i = 0; i < numSlots; ++i) {
        // Randomly pick a valid tuple to delete
        if ( hdr.isValidTuple(i) ) {
          t = checkTupleRetrieval(p, it, i);
          if  ( Math.random() > 0.5 ) {
            deletedSlots.add(i);
            t = null;
            checkTupleRemoval(hdr, it, i);
          }
        }
      }
      
      // Ensure a valid final iterator state.
      checkFinalIterator(t, it);

      // Ensure the number of slots has not changed after removals.
      assertTrue ( hdr.getNumSlots() == numSlots );

      // Ensure deleted slots are invalid.
      for (int i = 0; i < numSlots; ++i) {
        assertTrue ( deletedSlots.contains(i) != hdr.isValidTuple(i) );
      }
    }    
  }

}
