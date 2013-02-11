package edu.jhu.cs.damsl.catalog.identifiers;

import java.io.Serializable;

import edu.jhu.cs.damsl.catalog.Addressable;

public class TransactionId implements Addressable, Serializable {
  private static Integer txnCounter = 0;
  protected Integer txnId;

  public TransactionId() { txnId = txnCounter++; }

  @Override
  public int getAddress() { return txnId; }

  @Override
  public String getAddressString() { return "TN"+txnId; }

}
