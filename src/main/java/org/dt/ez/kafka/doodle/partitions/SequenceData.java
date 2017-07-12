/**
 * Author : ez
 * Date : 2017/7/12
 * 
 * Describe :
 *    We use this class to be an algorithm:
 *    	f(time_flag, tag) = <tag, times_flag + "," + current_timestamp>;
 *    Time_flag shell be increased automatically. Each tag has its' own
 *    Time_flag. So we use a TagInfo to stored the pair <time_flag, tag>;
 *    
 *    We just create a SequenceData instance by a starting time_flag. And
 *    register some tags. Then function getKafkaValue () shell return the
 *    expected <time_flag, tag> pair.
 */
package org.dt.ez.kafka.doodle.partitions;

import java.util.List;
import java.util.ArrayList;
import java.util.Random;

import org.dt.ez.common.utils.Pair;

public class SequenceData {
	
	private long start;
	
	private List <TagInfo> tags;
	private Random rand;
	
	public Pair <String, String> getKafkaValue () {
		Pair <String, String> res = new Pair <String, String> ();
		return getKafkaValue (res);
	}
	
	public Pair <String, String> getKafkaValue (Pair <String, String> output) {
		int x = rand.nextInt (tags.size ());
		TagInfo tag = tags.get (x);
		output.setFirst (tag.tag);
		output.setSecond (tag.current + "+" + tag.tag + "+" + System.currentTimeMillis ());
		tag.current ++;
		return output;
	}
	
	public void registerTags (String ... ts) {
		for (String tag : ts)
			registerTag (tag);
	}
	
	public SequenceData registerTag (String t) {
		tags.add (new TagInfo (t));
		return this;
	}
	
	public SequenceData (long start) {
		this.start = start;
		if (this.start < 0) this.start = 0;
		tags = new ArrayList <TagInfo> (3);
		rand = new Random ();
	}
	
	class TagInfo {
		public String tag;
		public long current;
		
		public TagInfo (String t) {
			this.tag = t; this.current = 0;
		}
	}
	
	/**
	 * Unit testing.
	 * 
	 * @param args
	 *  No usage.
	 */
	public static void main (String [] args) {
		String [] tags = {
			"a", "b", "c", "d"
		};
		
		long startTimeFlag = 100L;
		
		SequenceData seq = new SequenceData (startTimeFlag);
		seq.registerTags (tags);
		
		Pair <String, String> output = new Pair <String, String> ();
		
		int i = 0;
		while (i ++ < 10000) {
			System.out.println (seq.getKafkaValue (output));
		}
		
	}
	
	
}


