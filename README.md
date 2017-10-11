package com.zsj.control;

import java.util.List;

import org.apache.spark.api.java.JavaRDD;

import com.zsj.bean.CompareTable;

public class AnalyseAnswer {
	/**
	 * 判断是否有词语被错误的分开,如果有则合并，并取fre为最小
	 */
	public static List<CompareTable> mergeWords(JavaRDD<CompareTable>answer)
	{
		List<CompareTable>list=answer.collect();
		
		for(int i=0;i<list.size();i++)
		{
			int j=i+1;
			int count=0;//存放合并指标次数，这个词最好在config里面弄成设置
			int temp=i+1;//存放待合并词的位置
			
			if(i!=list.size()-1)
			{
				for(;j<list.size();j++)
				{
					if(list.get(j).key!=null&&list.get(i).backKey.equals(list.get(j).key)&&count<3)
					{
						count++;
						temp=j;
					}
				}
				if(count>3)//这里的3应该设置成配置文件配置，这样的话默认被分错的词fre>3才有资格做答案
				{
					list.get(i).key=list.get(i).key+list.get(j).key;//合并成词
					list.get(i).backKey=list.get(j).backKey;
					
					list.get(j).key=null;
				}
			}
			
		}
		
		
		
		return list;
		
	}
}


package com.zsj.bean;

import java.io.Serializable;

public class CompareTable implements Serializable {
	public long id ;
	public String key;//当前词汇
	public int fre;//当前词汇的频率
	public String frontKey;//当前词汇的前一个词汇
	public int frontFre;//当前词汇的前一个词汇的频率
	public String backKey;//当前词汇的后一个词汇
	public int backFre;//当前词汇的后一个词汇的频率
	
	public String getKey() {
		return key;
	}
	public void setKey(String key) {
		this.key = key;
	}
	public int getFre() {
		return fre;
	}
	public void setFre(int fre) {
		this.fre = fre;
	}
	public String getFrontKey() {
		return frontKey;
	}
	public void setFrontKey(String frontKey) {
		this.frontKey = frontKey;
	}
	public int getFrontFre() {
		return frontFre;
	}
	public void setFrontFre(int frontFre) {
		this.frontFre = frontFre;
	}
	public String getBackKey() {
		return backKey;
	}
	public void setBackKey(String backKey) {
		this.backKey = backKey;
	}
	public int getBackFre() {
		return backFre;
	}
	public void setBackFre(int backFre) {
		this.backFre = backFre;
	}
	
	
}
