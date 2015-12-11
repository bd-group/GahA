package iie.gaha.common;

public class QueryFlag {
	// accept Java Object result
	public static final byte ACCEPT_JAVA_OBJ = 0x01;
	// data is zipped by snappy
	public static final byte USE_SNAPPY = 0x02;
	// data is zipped by gzip
	public static final byte USE_GZIP = 0x04;
}
