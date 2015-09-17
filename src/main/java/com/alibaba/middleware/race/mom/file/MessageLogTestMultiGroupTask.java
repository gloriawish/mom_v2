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

/**
 * @author showki
 * 
 */
public class MessageLogTestMultiGroupTask {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
		// TODO Auto-generated method stub

		MessageLog log = new MessageLog("test1");
		long startTime = 0;
		long totalSynSaveTime = 0;
		List<SendTask> list = null;
		long totalAsynSaveTime = 0;
		startTime = System.currentTimeMillis();
		list = log.RestoreAllMultiGroupTask();
		totalAsynSaveTime += System.currentTimeMillis()-startTime;
		System.out.println("after restoring, size is:" + list.size());
		System.out.println("Restore:"+totalAsynSaveTime+" ms");
		int num = 10000;
		
		
		//TODO(yukai) 使用future特性和callable！！
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
			if (log.SynSaveMultiGroupTask(logGroupTasks) == false) {
				System.out.println("save to file failed");
			} else {
//				System.out.println("save to file successfully");
			}
			totalSynSaveTime += System.currentTimeMillis()-startTime;
		}
		System.out.println("SynSave :"+num*1000.0 / totalSynSaveTime);

/*		
		for (int i = 001; i < num/10; i++) {
			List<byte[]> blist = new Vector<byte[]>();
			
			for (int j = 0; j < 10; j++) {
				Message msg = new Message();
				msg.setMsgId(String.valueOf(i*10+j));
				msg.setBody("sfsefs".getBytes());

				SendTask sendTask = new SendTask();
				sendTask.setGroupId(String.valueOf(i*10+j));
				sendTask.setMessage(msg);

				LogTask logTask = new LogTask(sendTask, 0);

				ByteArrayOutputStream os = new ByteArrayOutputStream();
				ObjectOutputStream out = new ObjectOutputStream(os);
				out.writeObject(logTask);
				byte[] b = os.toByteArray();
				
				blist.add(b);
			}

			startTime = System.currentTimeMillis();
			if (log.SynSave(blist) == false)
				System.out.println("save to file failed");
			totalSynSaveTime += System.currentTimeMillis()-startTime;
		}
		System.out.println("SynSave by group:"+num * 1000.0/ totalSynSaveTime);
*/		
		
		totalAsynSaveTime = 0;
		for (int i = 0; i < num; i++) {
			if (i % 3 != 0)
				continue;

			Message msg = new Message();
			msg.setMsgId("1");
			msg.setBody("ssssssss".getBytes());

			startTime = System.currentTimeMillis();
			if (false == log.AsynSaveMultiGroupTask("topictesttest", msg, 1, String.valueOf(i))) {
				System.out.println("save to file failed");
			}
			totalAsynSaveTime += System.currentTimeMillis()-startTime;
		}
		System.out.println("AsynSave:"+num/3.0*1000/totalAsynSaveTime);

		totalAsynSaveTime = 0;
		startTime = System.currentTimeMillis();
		list = log.RestoreAllMultiGroupTask();
		totalAsynSaveTime += System.currentTimeMillis()-startTime;
		System.out.println("after restoring, size is:" + list.size());
		System.out.println("Restore:"+totalAsynSaveTime+" ms");

		// System.out.println(list.get(0).getMessage().getBody().toString());
//		 for (SendTask sendTask : list) {
//		 System.out.println(sendTask.getGroupId());
//		 }
	}

}
