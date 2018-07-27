package com.jun.util;

import java.io.Serializable;


public enum GenerateIdUtil implements Serializable {

	INSTANCE;

	public String getId() {
		return getId(1, 1);
	}

	public String getId(long workerId, long datacenterId) {
		return String.valueOf(new SnowflakeIdWorker(workerId, datacenterId).nextId());
	}
}
