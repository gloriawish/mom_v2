package com.alibaba.middleware.race.mom.broker.netty;

import io.netty.channel.Channel;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.SendResult;
import com.alibaba.middleware.race.mom.SendStatus;
import com.alibaba.middleware.race.mom.broker.AckManager;
import com.alibaba.middleware.race.mom.broker.ConsumerGroupInfo;
import com.alibaba.middleware.race.mom.broker.ConsumerManager;
import com.alibaba.middleware.race.mom.broker.SemaphoreManager;
import com.alibaba.middleware.race.mom.broker.SendHelper;
import com.alibaba.middleware.race.mom.broker.TaskManager;
import com.alibaba.middleware.race.mom.file.LogGroupTask;
import com.alibaba.middleware.race.mom.file.LogTask;
import com.alibaba.middleware.race.mom.model.SendTask;
import com.alibaba.middleware.race.mom.tool.MemoryTool;
import com.alibaba.middleware.race.mom.tool.Tool;


//broker收到producer的消息的时候监听类
public class ProducerMessageListener  extends MessageListener{

	@Override
	void onProducerMessageReceived(Message msg,String requestId,Channel channel) {
		
		//放入一个requestId对应的channel 在后面发送ack后删除
		AckManager.pushRequest(requestId, channel);
		
		// TODO Auto-generated method stub
		//标识是否序列化成功
		boolean isError=false;
		String mapstr="";
		for(Map.Entry<String, String> entry:msg.getProperties().entrySet()){    
			mapstr+=entry.getKey()+"="+entry.getValue();
		}   
		//System.out.println("receive producer message msgid:"+msg.getMsgId()+" topic:"+msg.getTopic()+" filter:"+mapstr);
		String topic=msg.getTopic();
		//找到订阅这个消息的组信息   最好有订阅过滤条件
		List<ConsumerGroupInfo> allgroups= ConsumerManager.findGroupByTopic(topic);
		//符合这个消息过滤消息的组
		List<ConsumerGroupInfo> groups=new ArrayList<ConsumerGroupInfo>();
		for (ConsumerGroupInfo groupinfo : allgroups) 
		{
			//groupinfo.findSubscriptionData(topic);
			String filterName=groupinfo.findSubscriptionData(topic).getFitlerName();
			String filterValue=groupinfo.findSubscriptionData(topic).getFitlerValue();
			if(filterName==null)
			{
				groups.add(groupinfo);
			}
			else
			{
				//判断消息是否有组需要的字段，且字段的值和消息一致
				if(msg.getProperty(filterName)!=null&&msg.getProperty(filterName).equals(filterValue))
				{
					groups.add(groupinfo);
				}
			}
		}
		if(groups.size()==0)//没有订阅这个消息的组，需要把消息存储在默认队列里面？
		{
			//TODO 丢失信息？
			//查找有没有专属于存储这个topic的队列
			//QueueFile queue=QueueManager.findQueue(topic);
			System.out.println("don't have match group");
			//返回这个ack
			SendResult ack=new SendResult();
			ack.setMsgId(msg.getMsgId());//message id
			ack.setInfo(requestId);//request id
			ack.setStatus(SendStatus.SUCCESS);
			AckManager.pushAck(ack);
			SemaphoreManager.increase("Ack");
		}
		//遍历所有的订阅该消息的组
		List<SendTask> taskList=new ArrayList<SendTask>();
		String[] groupids = new String[groups.size()];
		for (int i = 0; i < groups.size(); i++) {
			groupids[i]=groups.get(i).getGroupId();
			SendTask task=new SendTask();
			task.setGroupId(groups.get(i).getGroupId());
			task.setTopic(topic);
			task.setMessage(msg);
			
			taskList.add(task);
			
		}
		LogGroupTask logGroupTask = new LogGroupTask(topic, msg, 0, groupids);
		try 
		{
			String key=requestId+"@"+msg.getMsgId();
			//先把这些任务写到缓冲区,当刷盘操作完成后会生成对应消息的ack消息，然后通过单独的线程发送到生产者
			FlushTool.writeToCache(logGroupTask,key);
		}
		catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//添加一个发送任务
		if(MemoryTool.moreThan(1024*1024*100))//100MB可用内存
		{
			TaskManager.pushTask(taskList);//持久化结束后,把这些任务放到内存中的任务队列
			
			for (int i=0;i<taskList.size();i++) {
				SemaphoreManager.increase("SendTask");
			}
			
		}
		else
		{
			//不添加到发送队列了
			//TODO 存一份在专门的文件队列，在内存中队列的任务变少的时候加载进入内存
			for (int i=0;i<taskList.size();i++) {
				QueueStore.saveTask(taskList.get(i));
			}
			
		}
	}

}
