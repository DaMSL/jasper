package edu.jhu.cs.damsl.engine.storage.iterator.util;

import java.util.Iterator;
import java.util.LinkedList;

public class MultiplexedListIterator<T, TIterator extends Iterator<T>>
  implements Iterator<T>
{

  LinkedList<TIterator> list;
  Iterator<TIterator> listIt;
  Iterator<T> elementIt;

  public MultiplexedListIterator(LinkedList<TIterator> l) {
    list = l;
    if ( l != null ) {
      listIt = l.iterator();
      nextValidElement();
    }
  }
  
  void nextValidElement() {
    boolean r = ( elementIt == null? false : elementIt.hasNext() );
    while ( !r && listIt.hasNext() ) {
      elementIt = listIt.next();
      r = elementIt.hasNext();
    }
    if ( !r ) { elementIt = null; }    
  }
  
  @Override
  public boolean hasNext() {
    nextValidElement();
    return (elementIt != null && elementIt.hasNext());
  }
  
  @Override 
  public T next() { 
    if ( elementIt == null ) return null;
    return elementIt.next();
  }
  
  @Override
  public void remove() { 
    if ( elementIt != null ) elementIt.remove();
  }

}
