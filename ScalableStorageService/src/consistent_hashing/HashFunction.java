package consistent_hashing;

public interface HashFunction {
  public int hash(Object o);
}