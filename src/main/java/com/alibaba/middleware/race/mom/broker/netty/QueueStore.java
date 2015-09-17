package com.alibaba.middleware.race.mom.broker.netty;

import java.io.IOException;
import java.util.List;

import com.alibaba.middleware.race.mom.broker.SemaphoreManager;
import com.alibaba.middleware.race.mom.broker.TaskManager;
import com.alibaba.middleware.race.mom.file.FileQueue;
import com.alibaba.middleware.race.mom.file.LogTask;
import com.alibaba.middleware.race.mom.model.SendTask;
import com.alibaba.middleware.race.mom.tool.Tool;

/**
 * 这个队列是专门用于存储内存中存储不下的任务的，当内存中可以存储得下的时候加入内存
 * @author zz
 *
 */
public class QueueStore {
	private static FileQueue fileQueue = null;
	
	static
	{
		try {
			fileQueue=new FileQueue("filequeue");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void saveTask(SendTask task)
	{
		LogTask logTask = new LogTask(task, 0);
		byte[] b = Tool.serialize(logTask);
		fileQueue.SynSave(b);
	}
	
	public static void restoreTask(int number)
	{
		List<SendTask> list = fileQueue.Restore(number);
		for (SendTask sendTask : list) {

			TaskManager.pushTask(sendTask);
			SemaphoreManager.increase("SendTask");
		}
	}
	
	
}
