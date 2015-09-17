package com.alibaba.middleware.race.mom.broker.netty;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;
import com.alibaba.middleware.race.mom.broker.AckManager;
import com.alibaba.middleware.race.mom.broker.SemaphoreManager;
import com.alibaba.middleware.race.mom.file.LogGroupTask;
import com.alibaba.middleware.race.mom.file.MessageLog;
import com.alibaba.middleware.race.mom.tool.LogWriter;


/**
 * 
 * @author sei.zz
 *
 */
public class FlushTool {
	
	
	public static MessageLog log=null;
	public static LogWriter logWriter=null;
	static
	{
		try 
		{
			log=new MessageLog("message");//初始化持久化文件的实例
			logWriter=LogWriter.getLogWriter();
		}
		catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static Object syncObj=new Object();//用来刷磁盘的时候同步的
	
	//存储正在等待ack消息的requestID和message ID
	public static List<String> requestCacheList=new ArrayList<String>();
	//一段时间内收到的消息缓存在这里，由一个单独的线程来刷磁盘
	public static List<LogGroupTask> cacheList = new ArrayList<LogGroupTask>();
	
	public static Semaphore semp=null;//一个标识收到多少个数据后就开始刷盘的信号量。
	public static int threadNum=Conf.connNum;
	static
	{
		semp=new Semaphore(-threadNum);//与发送线程数量相同
	}
	
	public static void writeToCache(LogGroupTask data,String threadId)
	{
		//System.out.println("write in cache");
		synchronized (cacheList) {
			cacheList.add(data);
			requestCacheList.add(threadId);
			semp.release();
		}
	}
	
	public static void reset()
	{
		semp=new Semaphore(-threadNum);
	}
	
	//将缓冲区的数据写入硬盘，唤醒在等待刷盘操作的线程
	public static void flush()
	{
		List<LogGroupTask> temp=null;
		List<String> requestTemp=null;
		synchronized (cacheList) {
			temp=new ArrayList<LogGroupTask>();
			requestTemp=new ArrayList<String>();
			temp.addAll(cacheList);
			requestTemp.addAll(requestCacheList);
			cacheList.clear();
			requestCacheList.clear();
			reset();
		}
		
		if(temp!=null&&temp.size()>0)//刷已经写好的数据，此时其他线程可以把数据写入cacheList
		{
			long start=System.currentTimeMillis();
			boolean error=false;
			//同步的把数据存储到磁盘
			if(!log.SynSaveMultiGroupTask(temp))
				error=true;
			long end=System.currentTimeMillis();
			
			logWriter.log("save use time:"+(end-start)+" number:"+temp.size()+" cachelist:"+cacheList.size());
			for (int i = 0; i < requestTemp.size(); i++) {
				SendResult ack=new SendResult();
				String[] arr=requestTemp.get(i).split("@");
				ack.setMsgId(arr[1]);//message id
				ack.setInfo(arr[0]);//request id
				if(error)
					ack.setStatus(SendStatus.FAIL);
				else
					ack.setStatus(SendStatus.SUCCESS);
				
				AckManager.pushAck(ack);//往ack队列里面放入一个ack消息
				SemaphoreManager.increase("Ack");
			}
		}
		//System.out.println("flush");
	}
	
	public static boolean writeConsumeResult(String topic,Message msg,String groupId)
	{
		if(log!=null)
			return  log.AsynSaveMultiGroupTask(topic, msg, 1, groupId);
		else
			return false;
	}
}
