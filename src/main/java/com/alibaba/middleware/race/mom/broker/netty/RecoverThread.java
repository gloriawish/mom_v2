package com.alibaba.middleware.race.mom.broker.netty;

import com.alibaba.middleware.race.mom.broker.SemaphoreManager;
import com.alibaba.middleware.race.mom.broker.TaskManager;
import com.alibaba.middleware.race.mom.tool.MemoryTool;

/**
 * 定时从文件队列里面恢复出任务到内存中的的发送队列里面
 * 
 * 恢复的时候不需要往磁盘里刷了，因为之前已经刷到磁盘里面一次了，就算断电文件队列里面的数据丢失，也不会影响准确性，因为最后会
 * 从日志文件里面恢复出这些没有发送的任务
 * @author zz
 *
 */
public class RecoverThread  implements Runnable{

	@Override
	public void run() {
		// TODO Auto-generated method stub
		while(true)
		{
			if(MemoryTool.moreThan(1024*1024*100))//至少100MB可用内存
			{
				QueueStore.restoreTask(1000);//恢复1000个任务到内存中去
			}
			//每隔5秒恢复一次
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

}
