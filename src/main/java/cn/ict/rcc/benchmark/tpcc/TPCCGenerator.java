package cn.ict.rcc.benchmark.tpcc;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Data generator for tpcc benchmark
 * @author Wei
 *
 */
public class TPCCGenerator {

	public static int NURand(int A, int x, int y) {
		int c = 0;
		switch (A) {
		case TPCCConstants.A_C_LAST:
			c = TPCCConstants.C_C_LAST;
			break;
		case TPCCConstants.A_C_ID:
			c = TPCCConstants.C_C_ID;
			break;
		case TPCCConstants.A_OL_I_ID:
			c = TPCCConstants.C_OL_I_ID;
			break;
		default:
		}
		return (((randomInt(0, A) | randomInt(x, y)) + c) % (y - x + 1)) + x;
	}

	public static int randomInt(int min, int max) {
		return new Random().nextInt(max + 1) % (max - min + 1) + min;
	}

	public static float randomFloat(float min, float max) {
		return new Random().nextFloat() * (max - min) + min;
	}

	public static String makeAlphaString(int min, int max) {
		StringBuffer str = new StringBuffer();
		Random random = new Random();
		long result = 0;
		int number = random.nextInt(max) % (max - min + 1) + min;
		for (int i = 0; i < number; i++) {
			switch (random.nextInt(3)) {
			case 0: // CAP letter
				result = Math.round(Math.random() * 25 + 65);
				str.append(String.valueOf((char) result));
				break;
			case 1: // Low letter
				result = Math.round(Math.random() * 25 + 97);
				str.append(String.valueOf((char) result));
				break;
			case 2: // Number
				str.append(String.valueOf(new Random().nextInt(10)));
				break;
			}
		}
		return str.toString();
	}

	public static String makeNumberString(int min, int max) {
		StringBuffer str = new StringBuffer();
		Random random = new Random();
		int number = random.nextInt(max + 1) % (max - min + 1) + min;
		for (int i = 0; i < number; i++) {
			str.append(String.valueOf(new Random().nextInt(10)));
		}
		return str.toString();
	}

	public static String buildString(Object... args) {
		StringBuffer stringBuffer = new StringBuffer();
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof String) {
				stringBuffer.append((String) args[i]);
			} else {
				stringBuffer.append(String.valueOf(args[i]));
			}
		}
		return stringBuffer.toString();
	}

	public static List<String> buildColumns(Object... args) {
		List<String> columns = new ArrayList<String>();
		for (int i = 0; i < args.length; i++) {
			if (args[i] instanceof String) {
				columns.add((String) args[i]);
			} else {
				columns.add(String.valueOf(args[i]));
			}
		}
		return columns;
	}
	
	/*
	 * This function generates the last name for customers Argument : int num,
	 * String name
	 */
	public static String Lastname(int num) {
		String name = "";
		String n[] = { "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI",
				"CALLY", "ATION", "EING" };
		name += n[num / 100];
		name += n[(num / 10) % 10];
		name += n[num % 10];
		return name;
	}
}
