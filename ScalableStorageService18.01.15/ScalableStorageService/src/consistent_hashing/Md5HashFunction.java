package consistent_hashing;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public class Md5HashFunction implements HashFunction {

  private final MessageDigest md5;
  
  private static Md5HashFunction instance;

	private Md5HashFunction() throws NoSuchAlgorithmException {
		md5 = MessageDigest.getInstance("MD5");
	}

	public static synchronized Md5HashFunction getInstance() {
		if (instance == null) {
			try {
				instance = new Md5HashFunction();
			} catch (NoSuchAlgorithmException e) {
				// TODO Auto-generated catch block
			}
		}
		return instance;
	}
  
  
  public int hash(Object o) {
    byte[] bs = md5.digest(o.toString().getBytes());
    return ((bs[3] & 0xFF) << 24)
      | ((bs[2] & 0xFF) << 16)
      | ((bs[1] & 0xFF) << 8)
      | ((bs[0] & 0xFF));
  }

}