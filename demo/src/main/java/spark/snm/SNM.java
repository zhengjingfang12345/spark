package spark.snm;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import spark.snm.comparator.Sort;

public class SNM {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("map");
		JavaSparkContext sc = new JavaSparkContext(conf);
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

		DataRow row6 = new DataRow();
		row6.setEmail("3@163.com");
		row6.setIdCard("441522108209080134");
		row6.setImei("");
		row6.setPhone("13880438575");
		row6.setTaobaoId("taobao124@taobao.com");
		row6.setId(6);
		/////////////////////////////////////////////////////
		dataRows.add(row1);
		dataRows.add(row2);
		dataRows.add(row3);
		dataRows.add(row4);
		dataRows.add(row5);
		dataRows.add(row6);
		int sortTime2 = 2;
		for (int index = 0; index < sortTime2; index++) {
			if (index == 0) {
				Collections.sort(dataRows, new Sort(Sort.DEFAULT_KEY));
			} else {
				Collections.sort(dataRows, new Sort(Sort.IDCARD_KEY));
			}
			for (DataRow dataRow : dataRows) {
				System.out.println(dataRow.getId());
			}
			merge(dataRows);
		}

	}

	private static void merge(List<DataRow> dataRows) {
		int totalCount = dataRows.size();
		// 记录最后索引位
		int last = totalCount - 1;
		// 窗口大小
		int windowsSize = 2;
		int windowTop = 0;
		int windowBottom = windowTop + windowsSize - 1;
		while (windowBottom <= last) {
			DataRow bottomDataRow = dataRows.get(windowBottom);
			for (int index = windowTop; index < windowBottom; index++) {
				DataRow windowDataRow = dataRows.get(index);
				if (isSimilary(bottomDataRow, windowDataRow)) {
					System.out.println(
							"dataRowId:" + bottomDataRow.getId() + "," + windowDataRow.getId() + " is similary");
				}

			}
			windowTop++;
			windowBottom++;
		}
	}

	static Boolean isSimilary(DataRow row1, DataRow row2) {
		if (rule1(row1, row2) || rule2(row1, row2)) {
			return true;
		} else {
			return false;
		}

	}

	/**
	 * 规则1
	 * 
	 * @param row1
	 * @param row2
	 * @return
	 */
	static Boolean rule1(DataRow row1, DataRow row2) {
		if ((row1.getImei().equalsIgnoreCase(row2.getImei()) && row1.getPhone().equalsIgnoreCase(row2.getPhone())
				&& row1.getTaobaoId().equalsIgnoreCase(row2.getTaobaoId()))) {
			return true;
		} else

		{
			return false;
		}

	}

	/**
	 * 规则2
	 * 
	 * @param row1
	 * @param row2
	 * @return
	 */
	static Boolean rule2(DataRow row1, DataRow row2) {
		if ((row1.getIdCard().equalsIgnoreCase(row2.getIdCard())) && !row1.getIdCard().isEmpty()
				&& !row2.getIdCard().isEmpty()) {
			return true;
		} else

		{
			return false;
		}

	}
}
