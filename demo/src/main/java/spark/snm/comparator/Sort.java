package spark.snm.comparator;

import java.util.Comparator;

import spark.snm.DataRow;

public class Sort implements Comparator<DataRow> {

	public final static int IDCARD_KEY = 1;
	public final static int DEFAULT_KEY = -1;
	private int state;

	public Sort(int state) {
		this.state = state;
	}

	public Sort() {

	}

	public int compare(DataRow o1, DataRow o2) {
		if (state == Sort.DEFAULT_KEY) {
			return sortByDefault(o1, o2);
		}
		return sortByIdCard(o1, o2);
	}

	private int sortByDefault(DataRow o1, DataRow o2) {
		String key1 = o1.getImei() + o1.getPhone() + o1.getTaobaoId();
		String key2 = o2.getImei() + o2.getPhone() + o2.getTaobaoId();
		if (key1.compareTo(key2) > 0) {
			return -1;
		} else if (key1.compareTo(key2) < 0) {
			return 1;
		} else {
			return 0;
		}
	}

	private int sortByIdCard(DataRow o1, DataRow o2) {
		String idCard1 = o1.getIdCard();
		String idCard2 = o2.getIdCard();
		if (idCard1.compareTo(idCard2) < 0) {
			return -1;
		} else if (idCard1.compareTo(idCard2) > 0) {
			return 1;
		} else {
			return 0;
		}
	}
}