package iu.pti.hbaseapp.clueweb09;

import iu.pti.hbaseapp.Constants;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class FreqIndexBuilderClueWeb09 {
	/**
	 * Internal Mapper to be run by Hadoop.
	 */
	public static class FibMapper extends TableMapper<ImmutableBytesWritable, Writable> {		
		@Override
		protected void map(ImmutableBytesWritable rowKey, Result result, Context context) throws IOException, InterruptedException {
			byte[] docIdBytes = rowKey.get();
			byte[] contentBytes = result.getValue(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES);
			String content = Bytes.toString(contentBytes);
			
			HashMap<String, Integer> x = getTermFreqs(content);	

			for(String a : x.keySet()) {
				//context.write(new Text(a), new LongWritable(x.get(a)));
				//This is the "word" in the document which is the rowkey in new table
				byte[] rowKey1 = Bytes.toBytes(a.toString());
				//This is the frequency of word in document
				byte[] contentBytes1 = Bytes.toBytes(x.get(a));
				//The docID is the document id.
				byte[] docID = Bytes.toBytes(docIdBytes.toString());
				Put put = new Put(rowKey1);
				put.add(Constants.CF_FREQUENCIES_BYTES, docID, contentBytes);
				context.write(new ImmutableBytesWritable(put.getRow()), put);
			}			
		}
	}
	
	/**
	 * get the terms, their frequencies and positions in a given string using a Lucene analyzer
	 * @param text
	 * 
	 */
	public static HashMap<String, Integer> getTermFreqs(String text) {
		HashMap<String, Integer> freqs = new HashMap<String, Integer>();
		try {
			Analyzer analyzer = Constants.analyzer;
			TokenStream ts = analyzer.reusableTokenStream("dummyField", new StringReader(text));
			CharTermAttribute charTermAttr = ts.addAttribute(CharTermAttribute.class);
			while (ts.incrementToken()) {
				String termVal = charTermAttr.toString();
				if (Helpers.isNumberString(termVal)) {
					continue;
				}
				
				if (freqs.containsKey(termVal)) {
					freqs.put(termVal, freqs.get(termVal)+1);
				} else {
					freqs.put(termVal, 1);
				}
			}
			ts.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return freqs; 
	}
	
	
	/**
	 * Job configuration.
	 */
	public static Job configureJob(Configuration conf, String[] args) throws IOException {
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.reduce.tasks.speculative.execution", "false");
	    Scan scan = new Scan();
	    scan.addColumn(Constants.CF_DETAILS_BYTES, Constants.QUAL_CONTENT_BYTES);
		Job job = new Job(conf,	"Building freq_index from " + Constants.CLUEWEB09_DATA_TABLE_NAME);
		job.setJarByClass(FibMapper.class);
		TableMapReduceUtil.initTableMapperJob(Constants.CLUEWEB09_DATA_TABLE_NAME, scan, FibMapper.class, ImmutableBytesWritable.class, Writable.class, job, true);
		TableMapReduceUtil.initTableReducerJob(Constants.CLUEWEB09_INDEX_TABLE_NAME, null, job);
		job.setNumReduceTasks(0);
		
		return job;
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		Job job = configureJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}