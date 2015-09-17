package com.alibaba.middleware.race.mom.file;

public class FileWRFactory {
	public FileWRFactory() {
		// TODO Auto-generated constructor stub
	}
	FileHandler GetDefaultFileHandler(String filePath) {
//		return new DefaulteFileHandler();
		assert(false);
		return null;
	}
	
	FileHandler getRandomAccessFileHandler() {
		return new RandomAccessFileHandler();
	}
	
	FileHandler getChannelFileHandler() {
		return new ChannelFileHander();
	}
	
	FileHandler getMapChannelFileHandler() {
		return new MapChannelFileHandler();
	}
}
