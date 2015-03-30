package cn.ict.dtcc.util;

public class RccUtil {

	public static String buildString(String... args) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < args.length; i++) {
			stringBuffer.append((String) args[i]);
		}
		return stringBuffer.toString();
	}

}
