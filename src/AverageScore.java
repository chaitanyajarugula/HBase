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



public class AverageScore {

	public static String Table_Name = "Movies";
	
	public static void main(String[] argv) throws Exception {
		Configuration conf = HBaseConfiguration.create();        
		@SuppressWarnings({ "deprecation", "resource" })
		HTable hTable = new HTable(conf, Table_Name);
		Scan scan = new Scan();
		//scan.setFilter(allFilters);
		
		ResultScanner scanner = hTable.getScanner(scan);
		double row_count = 0;
		double total_score =0;
		for(Result result=scanner.next(); result!=null; result=scanner.next()) {
			row_count++;
			String score=new String(result.getValue(
					Bytes.toBytes("Product"),
					Bytes.toBytes("score")));
			total_score+= Double.parseDouble(score);
			//System.out.println("current_score:"+String.valueOf(total_score)+"Score"+score+"recordcount: "+row_count);
		}
		System.out.println("Average Score: "+ total_score/row_count);
    }
}
