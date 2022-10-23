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



public class ScoreCount {

	public static String Table_Name = "Movies";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE) ;
		SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("score"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("0.0")));
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("score"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("3.0")));
		SingleColumnValueFilter filter3 = new SingleColumnValueFilter(
				Bytes.toBytes("Product"), 
				Bytes.toBytes("score"),
				CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes("5.0")));
		allFilters.addFilter(filter1);
		allFilters.addFilter(filter2);
		allFilters.addFilter(filter3);
		Scan scan = new Scan();
		scan.setFilter(allFilters);
		
		ResultScanner scanner = hTable.getScanner(scan);
		int row_count = 0;
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			row_count++;
			String score=new String(result.getValue(
					Bytes.toBytes("Product"),
					Bytes.toBytes("score")));
			//System.out.println("score:"+score);
		}
		System.out.println("number of rows "+ row_count);
    }
}
