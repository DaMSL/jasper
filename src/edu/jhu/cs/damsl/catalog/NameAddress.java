package edu.jhu.cs.damsl.catalog;

import java.io.Serializable;

public class NameAddress implements Addressable, Serializable {
  String name;

  public NameAddress(String n) { name = n; }

  @Override
  public int getAddress() { return name.hashCode(); }

  @Override
  public String getAddressString() { return name; }

  @Override
  public String toString() { return name; }
}
