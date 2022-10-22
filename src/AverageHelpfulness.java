import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;



public class AverageHelpfulness {

	public static String Table_Name = "Movies";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		Scan scan = new Scan();
		ResultScanner scanner = hTable.getScanner(scan);
		double row_count = 0;
		double helpfulness_fraction =0;
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			row_count++;
			String helpfulness=new String(result.getValue(
					Bytes.toBytes("Product"),
					Bytes.toBytes("helpfulness")));
			String [] values = helpfulness.split("/");
			double accepted = Double.parseDouble(values[0]);
			double total = Double.parseDouble(values[1]);
			if(total==0)
			helpfulness_fraction+=0;
			else
			helpfulness_fraction+= (accepted/total);
			System.out.println("current_score:"+String.valueOf(helpfulness_fraction)+"Score"+helpfulness+"recordcount: "+row_count);
		}
		System.out.println(helpfulness_fraction/row_count);
    }
}
