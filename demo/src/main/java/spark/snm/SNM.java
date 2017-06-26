package spark.snm;

import java.util.ArrayList;
import java.util.List;

public class SNM {

	public static void main(String[] args) {

		List<DataRow> dataRows = new ArrayList<DataRow>();
		DataRow row1 = new DataRow();
		row1.setEmail("1@163.com");
		row1.setIdCard("441522108209080131");
		row1.setImei("AAA-123-abc");
		row1.setPhone("13880438571");
		row1.setTaobaoId("taobao123@taobao.com");
		row1.setId(1);

		DataRow row2 = new DataRow();
		row2.setEmail("");
		row2.setIdCard("");
		row2.setImei("AAA-123-abc");
		row2.setPhone("13880438571");
		row2.setTaobaoId("taobao123@taobao.com");
		row2.setId(2);
		/////////////////////////////////////////////////////
		DataRow row3 = new DataRow();
		row3.setEmail("1@163.com");
		row3.setIdCard("441522108209080133");
		row3.setImei("");
		row3.setPhone("13880438572");
		row3.setTaobaoId("taobao123@taobao.com");
		row3.setId(3);
		/////////////////////////////////////////////////////
		DataRow row4 = new DataRow();
		row4.setEmail("2@163.com");
		row4.setIdCard("441522108209080134");
		row4.setImei("BBB-123-abc");
		row4.setPhone("13880438575");
		row4.setTaobaoId("taobao124@taobao.com");
		row4.setId(4);

		DataRow row5 = new DataRow();
		row5.setEmail("2@163.com");
		row5.setIdCard("");
		row5.setImei("BBB-123-abc");
		row5.setPhone("13880438575");
		row5.setTaobaoId("taobao124@taobao.com");
		row5.setId(5);
		/////////////////////////////////////////////////////
		dataRows.add(row1);
		dataRows.add(row2);
		dataRows.add(row3);
		dataRows.add(row4);
		dataRows.add(row5);

		int totalCount = dataRows.size();
		int last = totalCount - 1;
		int windowsSize = 2;
		int windowTop = 0;
		int windowBottom = windowTop + windowsSize - 1;
		int pointer = windowBottom;
		while (windowBottom <= last) {
			DataRow pointerDataRow = dataRows.get(pointer);
			for (int index = windowTop; index < windowBottom; index++) {
				DataRow windowDataRow = dataRows.get(index);
				if (isSimilary(pointerDataRow, windowDataRow)) {
					System.out.println(
							"dataRowId:" + pointerDataRow.getId() + "," + windowDataRow.getId() + " is similary");
				}

			}
			windowTop++;
			windowBottom++;
			pointer++;

		}
	}

	static Boolean isSimilary(DataRow row1, DataRow row2) {
		if (row1.getImei().equalsIgnoreCase(row2.getImei()) && row1.getPhone().equalsIgnoreCase(row2.getPhone())
				&& row1.getTaobaoId().equalsIgnoreCase(row2.getTaobaoId())) {
			return true;
		} else {
			return false;
		}

	}

}
