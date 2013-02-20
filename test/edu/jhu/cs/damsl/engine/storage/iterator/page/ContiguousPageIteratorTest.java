package edu.jhu.cs.damsl.engine.storage.iterator.page;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import edu.jhu.cs.damsl.catalog.Schema;
import edu.jhu.cs.damsl.engine.storage.Tuple;
import edu.jhu.cs.damsl.engine.storage.page.ContiguousPage;
import edu.jhu.cs.damsl.engine.storage.page.PageHeader;
import edu.jhu.cs.damsl.utils.CommonTestUtils;
import edu.jhu.cs.damsl.utils.ContiguousPageTestUtils;

public class ContiguousPageIteratorTest {

  private ContiguousPageTestUtils ptUtils;
  private Schema schema;
  private List<Tuple> tuples;
  private List<ContiguousPage> testPages;
  private static final int numTuples = 5;

  @Before
  public void setUp() {
    ptUtils = new ContiguousPageTestUtils();
    schema = CommonTestUtils.getLIDSchema();
    tuples = ptUtils.getTuples(schema, numTuples);
    testPages = ptUtils.generateContiguousPages(false, schema, tuples);
  }

  void checkInitialIterator(ContiguousPage p, ContiguousPageIterator it) {
    // The iterator should refer to a valid tuple, and not have
    // yet returned a tuple.
    assertTrue ( it.getPage() == p
                  && it.getId() == p.getId()
                  && it.isCurrentValid() && !it.isReturnedValid() );
  }

  void checkFinalIterator(Tuple t, ContiguousPageIterator it) {
    assertTrue( !it.hasNext() && !it.isCurrentValid()
                  && (t != null) == (it.isReturnedValid()) );
  }

  Tuple checkTupleRetrieval(ContiguousPage p,
                            ContiguousPageIterator it, short offset)
  {
    Tuple tPage = p.getTuple(offset);
    assertTrue( it.hasNext() && it.isCurrentValid() );
    Tuple t = it.next();
    assertTrue( t != null && tPage.equals(t) && it.isReturnedValid() );
    return t;
  }
  
  void checkTupleRemoval(PageHeader hdr, ContiguousPageIterator it)
  {
    short prevUsage = hdr.getUsedSpace();
    it.remove();
    assertTrue( !it.isReturnedValid() && hdr.getUsedSpace() < prevUsage );
  }

  @Test
  public void resetTest() {
    for (ContiguousPage p : testPages) {
      Tuple t = null;
      ContiguousPageIterator it = p.iterator();
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
    for (ContiguousPage p : testPages) {
      ContiguousPageIterator it = p.iterator();
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
    for (ContiguousPage p : testPages) {
      Tuple t = null;
      ContiguousPageIterator it = p.iterator();
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
    for (ContiguousPage p : testPages) {
      Tuple t = null;
      PageHeader hdr = p.getHeader();
      short tupleSize = hdr.getTupleSize();
      
      ContiguousPageIterator it = p.iterator();
      checkInitialIterator(p,it);
      
      short offset = hdr.filledBackward()?
          (short) (hdr.getCapacity() - tupleSize) : hdr.getHeaderSize();

      for (; hdr.filledBackward()?
              offset >= hdr.getFreeSpaceOffset()
              : offset < hdr.getFreeSpaceOffset(); )
      {
        t = checkTupleRetrieval(p, it, offset);
        offset += (hdr.filledBackward()? -1 : 1) * tupleSize;
      }

      checkFinalIterator(t, it);
    }    
  }
  
  @Test
  public void removeTest() {
    for (ContiguousPage p : testPages) {
      PageHeader hdr = p.getHeader();
      short tupleSize = hdr.getTupleSize();
      
      ContiguousPageIterator it = p.iterator();
      checkInitialIterator(p,it);
      
      short offset = hdr.filledBackward()?
          (short) (hdr.getCapacity() - tupleSize) : hdr.getHeaderSize();

      for (; hdr.getUsedSpace() > 0; ) {
        checkTupleRetrieval(p, it, offset);
        checkTupleRemoval(hdr, it);
      }

      assertTrue( hdr.getUsedSpace() == 0 );
      checkFinalIterator(null, it);
    }
  }
  
  @Test
  public void removeRandomTest() {
    for (ContiguousPage p : testPages) {
      Tuple t = null;
      PageHeader hdr = p.getHeader();
      short tupleSize = hdr.getTupleSize();

      int numRemoved = 0;
      short initialSize = hdr.getUsedSpace();
      
      ContiguousPageIterator it = p.iterator();
      checkInitialIterator(p,it);
      
      short offset = hdr.filledBackward()?
          (short) (hdr.getCapacity() - tupleSize) : hdr.getHeaderSize();

      for (; hdr.filledBackward()?
                offset >= hdr.getFreeSpaceOffset()
                : offset < hdr.getFreeSpaceOffset(); )
      {
        t = checkTupleRetrieval(p, it, offset);
        if  ( Math.random() > 0.5 ) {
          t = null;
          numRemoved += 1;
          checkTupleRemoval(hdr, it);
        } else {
          offset = hdr.getNextTupleOffset(offset);
        }
      }

      assertTrue( hdr.getUsedSpace() == (initialSize - numRemoved*tupleSize) );
      checkFinalIterator(t, it);
    }
  }

}
