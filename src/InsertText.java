
import java.io.FileReader;
import java.io.IOException;
import java.util.stream.Stream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.util.Arrays; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class InsertText extends Configured implements Tool{
	public String Table_name = "Movies";
	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] argv) throws IOException{
		Configuration conf = HBaseConfiguration.create();
		@SuppressWarnings("resource")
		HBaseAdmin admin = new HBaseAdmin(conf);
		boolean isExists = admin.tableExists(Table_name);
		if(isExists == false) {
			HTableDescriptor htb = new HTableDescriptor(Table_name);
			HColumnDescriptor UserFamily = new HColumnDescriptor("User");
			HColumnDescriptor ProductFamily = new HColumnDescriptor("Product");
			htb.addFamily(UserFamily);
			htb.addFamily(ProductFamily);
			admin.createTable(htb);
		}
		@SuppressWarnings("resource")
		BufferedReader br = new BufferedReader(new FileReader("movies.txt"));
		String line="";
		String[] store = new String[8];
		int index = 0;
		int count_million = 0;
		int rows = 0;
		try {
		while((line = br.readLine())!=null || index==8){
			count_million+=1;
			if(index==8) 
			{//System.out.println(Arrays.toString(store)); 
			String productId = store[0];
			String userId = store[1];
			String profileName = store[2];
			String helpfulness = store[3];
			String score = store[4];
			String time = store[5];
			String summary = store[6];
			String text = store[7];
			index = 0;
			//System.out.println(productId);
			Put put = new Put(Bytes.toBytes(productId.concat(userId).concat(time)));
			put.add(Bytes.toBytes("Product"), Bytes.toBytes("ProductId"), Bytes.toBytes(productId));
			put.add(Bytes.toBytes("User"), Bytes.toBytes("UserId"), Bytes.toBytes(userId));
			put.add(Bytes.toBytes("User"), Bytes.toBytes("profileName"), Bytes.toBytes(profileName));
			put.add(Bytes.toBytes("Product"), Bytes.toBytes("helpfulness"), Bytes.toBytes(helpfulness));
			put.add(Bytes.toBytes("Product"), Bytes.toBytes("score"), Bytes.toBytes(score));
			put.add(Bytes.toBytes("Product"), Bytes.toBytes("time"), Bytes.toBytes(time));
			put.add(Bytes.toBytes("Product"), Bytes.toBytes("summary"), Bytes.toBytes(summary));
			put.add(Bytes.toBytes("Product"), Bytes.toBytes("text"), Bytes.toBytes(text));
	    	HTable hTable = new HTable(conf, Table_name);
	    	hTable.put(put);
	    	hTable.close();
	    	rows++;
			if(count_million==900000)
				break;
			continue;
			}			
			String[] item = line.split(": ", 2);
			store[index++] = item[item.length-1];
		}	
		}
		catch(Exception e) {
			System.out.println(Arrays.toString(store));
			System.out.print(line);
			e.printStackTrace();
			
		}
		System.out.println("loaded:" +rows+" rows" );
		return 0;
	}
    public static void main(String[] argv) throws Exception {
        int ret = ToolRunner.run(new InsertText(), argv);
        System.exit(ret);
    }
}