import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class MyClient {
	static final byte [] INFO_COLUMNFAMILY = Bytes.toBytes("attribute");
	static final byte [] NAME_QUALIFIER = Bytes.toBytes("name");

	public static Map<String, String> getPeopleInfo(HTable table, 
		String peopleId) throws IOException {
		Get get = new Get(Bytes.toBytes(peopleId)); // row ID
		get.addFamily(INFO_COLUMNFAMILY);			// specify column family
		Result res = table.get(get);
		if (res == null) {
			return null;
		}
		Map<String, String> resultMap = new HashMap<String, String>();
		resultMap.put("name", getValue(res, INFO_COLUMNFAMILY, NAME_QUALIFIER));
		return resultMap;
	}

	private static String getValue(Result res, byte[] cf, byte[] qualifier) {
		byte [] value = res.getValue(cf, qualifier);
		return value == null? "": Bytes.toString(value);
	}

	public static void main(String[] args) throws Exception {
		if (args.length != 1) {
			System.err.println("Usage: MyClient <people_id>");
			return;
		}
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "people"); // table name = 'people'

		Map<String, String> peopleInfo = getPeopleInfo(table, args[0]);

		if (peopleInfo == null) {
			System.err.printf("Station ID %s not found.\n", args[0]);
			return;
		}
		for (Map.Entry<String, String> people : peopleInfo.entrySet()) {
			// Print the date, time, and temperature
			System.out.printf("%s\t%s\n", people.getKey(), people.getValue());
		}
	}
}
