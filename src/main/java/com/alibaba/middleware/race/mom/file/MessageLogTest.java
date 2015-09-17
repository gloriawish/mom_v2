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
public class MessageLogTest {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public static void main(String[] args) throws IOException,
			ClassNotFoundException {
		// TODO Auto-generated method stub

		MessageLog log = new MessageLog("test1");
		long startTime = System.currentTimeMillis();
		long totalSynSaveTime = 0;
		
		List<SendTask> list = log.Restore();
		System.out.println("size is:" + list.size());
		
		int num = 100;
		
		
		//TODO(yukai) 使用future特性和callable！！

		for (int i = 0; i < num; i++) {
			Message msg = new Message();
			msg.setMsgId(String.valueOf(i));
			msg.setBody("sfsefs".getBytes());

			SendTask sendTask = new SendTask();
			sendTask.setGroupId(String.valueOf(i));
			sendTask.setMessage(msg);

			LogTask logTask = new LogTask(sendTask, 0);

			ByteArrayOutputStream os = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(os);
			out.writeObject(logTask);
			byte[] b = os.toByteArray();

			startTime = System.currentTimeMillis();
			if (log.SynSave(b) == false)
				System.out.println("save to file failed");
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
		
		long totalAsynSaveTime = 0;
		for (int i = 0; i < num; i++) {
			LogTask logTask = null;
			if (i % 3 == 0) {
				Message msg = new Message();
				msg.setMsgId(String.valueOf(i));
				msg.setBody("ssssssss".getBytes());

				SendTask sendTask = new SendTask();
				sendTask.setGroupId(String.valueOf(i));
				sendTask.setMessage(msg);

				logTask = new LogTask(sendTask, 1);
			} else {
				continue;
			}

			ByteArrayOutputStream os = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(os);
			out.writeObject(logTask);
			byte[] b = os.toByteArray();

			startTime = System.currentTimeMillis();
			if (false == log.AsynSave(b)) {
				System.out.println("save to file failed");
			}
			totalAsynSaveTime += System.currentTimeMillis()-startTime;
		}
		
		System.out.println("AsynSave:"+num/3.0*1000/totalAsynSaveTime);

//		assert(false);
//		List<SendTask> 
		totalAsynSaveTime = 0;
		startTime = System.currentTimeMillis();
		list = log.Restore();
		totalAsynSaveTime += System.currentTimeMillis()-startTime;
		System.out.println("after restoring, size is:" + list.size());
		System.out.println("Restore:"+totalAsynSaveTime+" ms");

		// System.out.println(list.get(0).getMessage().getBody().toString());
//		 for (SendTask sendTask : list) {
//		 System.out.println(sendTask.getGroupId());
//		 }
	}

}
