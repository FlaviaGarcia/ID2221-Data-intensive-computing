package id2221.topten;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

public class TopTen {
	// This helper function parses the stackoverflow into a Map for us.
	public static Map<String, String> transformXmlToMap(String xml) {
		Map<String, String> map = new HashMap<String, String>();
		try {
			String[] tokens = xml.trim().substring(5, xml.trim().length() - 3).split("\"");
			for (int i = 0; i < tokens.length - 1; i += 2) {
				String key = tokens[i].trim();
				String val = tokens[i + 1];
				map.put(key.substring(0, key.length() - 1), val);
			}
		} catch (StringIndexOutOfBoundsException e) {
			System.err.println(xml);
		}

		return map;
	}

	public static class TopTenMapper extends Mapper<Object, Text, NullWritable, Text> {
		// Stores a map of user reputation to the record
		TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		Map<String, String> user_map =  transformXmlToMap(value.toString());
		
		String user_id = user_map.get("Id");
		String user_reputation = user_map.get("Reputation");
		// first check that Id != null 
		if (user_id != null){
			repToRecordMap.put(Integer.parseInt(user_reputation), value);
		}
	}

	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Output our ten records to the reducers with a null key
		for (int i=0; i<10; i++){
			Map.Entry<Integer, Text> entry = repToRecordMap.pollLastEntry()
			context.write(NullWrittable.get(), entry.getValue());
		}	
	}
	}

	public static class TopTenReducer extends TableReducer<NullWritable, Text, NullWritable> {
		// Stores a map of user reputation to the record
		private TreeMap<Integer, Text> repToRecordMap = new TreeMap<Integer, Text>(Collections.reverseOrder());

	public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		try{
			// everything is in values cause the key of everything is = null  
			for (Text user_text: values){
				Map<String, String> user_map =  transformXmlToMap(user_text.toString());
				String user_reputation = user_map.get("Reputation");
				String user_id = user_map.get("id");
				repToRecordMap.put(Integer.parseInt(user_reputation), new Text(user_id));
			}
			// create again the TreeMap and limit it to 10 
			for (int i=0; i<10; i++){
				Map.Entry<Integer, Text> entry = repToRecordMap.pollLastEntry()
				Put insHBase = new Put(Integer.toString(i).getBytes());
				String rep = emtry.getKey().toString();
				String id = entry.getValue().toString();
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rep"), Bytes.toBytes(rep));
				insHBase.addColumn(Bytes.toBytes("info"), Bytes.toBytes("id"), Bytes.toBytes(id));
				context.write(NullWritable.get(), insHBase);
			}		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		
		// define scan and define column families to scan
		//Scan scan = new Scan();
		//scan.addFamily(Bytes.toBytes("info"));
		
		Job job = Job.getInstance(conf, "topten");
		job.setNumReduceTasks(1);
		job.setJarByClass(TopTen.class);
		job.setMapperClass(TopTenMapper.class);
		job.setMapOutputKey();

		// define input file
		FileInputFormat.addInputPath(job, new Path(args[0]))

		// define output table
		TableMapReduceUtil.initTableReducerJob("topten", TopTenReducer.class, job);
		
		job.waitForCompletion(true);
    }
}
