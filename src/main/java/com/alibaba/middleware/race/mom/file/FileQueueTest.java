/**
 * 
 */
package com.alibaba.middleware.race.mom.file;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Vector;

import com.alibaba.middleware.race.mom.Message;
import com.alibaba.middleware.race.mom.model.SendTask;
import com.alibaba.middleware.race.mom.tool.Tool;

/**
 * @author showki
 *
 */
public class FileQueueTest {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FileQueue fileQueue = new FileQueue("filequeue");
		int num  = 10000;
		long startTime = 0;
		long totalSynSaveTime = 0;
		
		for (int i = 0; i < num; i++) {
			Message msg = new Message();
			msg.setMsgId(String.valueOf(i));
			msg.setBody("sfsefs".getBytes());

			SendTask sendTask = new SendTask();
			sendTask.setGroupId(String.valueOf(i));
			sendTask.setMessage(msg);

			LogTask logTask = new LogTask(sendTask, 0);

//			ByteArrayOutputStream os = new ByteArrayOutputStream();
//			ObjectOutputStream out = new ObjectOutputStream(os);
//			out.writeObject(logTask);
			byte[] b = Tool.serialize(logTask);

			startTime = System.currentTimeMillis();
			if (fileQueue.SynSave(b) == false)
				System.out.println("save to file failed");
			totalSynSaveTime += System.currentTimeMillis()-startTime;
		}
		System.out.println("SynSave :"+num*1000.0 / totalSynSaveTime);

		
		List<SendTask> list = fileQueue.Restore(2);
		System.out.println(list.size());
		for (SendTask sendTask : list) {
			System.out.println(sendTask.getGroupId());
		}
		
		for (int i = 0; i < 20; i++) {
			list = fileQueue.Restore(num/20);
			System.out.println(list.size());
		}
		System.out.println(list.get(list.size()-1).getGroupId());

		//////////////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////////////
		//////////////////////////////////////////////////////////////////////////////
		
		
		FileQueue fileQueue2 = new FileQueue("filequeue2");
		int listNum = 5;
		int groupNum = 10;
		for (int i = 0; i < num/groupNum/listNum; i++) {
			Message msg = new Message();
			msg.setMsgId("1");
			msg.setBody("sfsefs".getBytes());

			List<LogGroupTask> logGroupTasks = new Vector<LogGroupTask>();
			for (int j = 0; j < listNum; j++) {
				String[] groups = new String[groupNum];
				for (int k = 0; k < groupNum; k++) {
					groups[k] = String.valueOf(i*groupNum*listNum+j*groupNum+k);
				}
				LogGroupTask logGroupTask = new LogGroupTask("topictesttest", msg, 0, groups);
				logGroupTasks.add(logGroupTask);
			}
			
			startTime = System.currentTimeMillis();
			if (fileQueue2.AsynSaveMultiGroupTask(logGroupTasks) == false) {
				System.out.println("save to file failed");
			} else {
//				System.out.println("save to file successfully");
			}
			totalSynSaveTime += System.currentTimeMillis()-startTime;
		}
		System.out.println("SynSave :"+num*1000.0 / totalSynSaveTime);
		
		
		List<SendTask> list1 = fileQueue2.RestoreMultiGroupTask(2);
		System.out.println(list1.size());
		for (SendTask sendTask : list1) {
			System.out.print(sendTask.getGroupId());
		}
		System.out.println("");

		for (int i = 0; i < num/groupNum/10; i++) {
			list1 = fileQueue2.RestoreMultiGroupTask(10);
			System.out.println(list1.size());
		}


		for (SendTask sendTask : list1) {
			System.out.print(sendTask.getGroupId()+"  ");
		}
	}

}
