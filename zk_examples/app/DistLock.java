import java.util.List;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;

public class DistLock implements Watcher
{
	private String groupName = null;
	private ZooKeeper zk = null;
	private boolean done = false;
	private Integer inLock = null;

	/**
	 * Constructor
	 */
	public DistLock()
	{
		inLock = new Integer(0);
	}

	/**
	 * Following OO-design: set the group path name.
	 */
	public void setName(String name)
	{
		this.groupName = name;
	}

	/**
	 * Get the lock!
	 * Relying on the Watcher trigger set of exists() method.
	 */
	public void get() throws KeeperException, InterruptedException
	{
		String path = "/" + groupName;

		while(!done)
		{
			try
			{
				String createdPath =
					zk.create(path,
						null /*data*/,
						Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL);
				done = true;
				System.out.println("Znode created: " + createdPath);
			}
			catch(KeeperException.NodeExistsException e)	// Important to our design...
			{
				try
				{
					// Let's set the watcher!
					Stat st = zk.exists(path, this);
					System.out.println("Watcher registered!");

					synchronized (inLock)
					{
						inLock.wait();	// Locking this thread...
					}
				}
				catch(KeeperException.NoNodeException e2)
				{
					System.out.printf("Group %s does not exist\n", groupName);
				}
			}
		}
	}

	public synchronized void process(WatchedEvent event)
	{
		// Unlock when the event type is NodeDeleted, but I don't want to check...
		synchronized (inLock)
		{
			inLock.notify();
		}
	}

	public void connect(String hostname)
	{
		try
		{
			zk = new ZooKeeper(hostname, 5000, this);
		}
		catch(Exception e)
		{
			System.err.println(e);
		}
	}

	public void close()
	{
		try
		{
			if(this.done)
				zk.close();
		}
		catch(Exception e)
		{
			System.err.println(e);
		}
	}

	public static void main(String[] args) throws Exception
	{
		if(args.length != 2)
		{
			System.err.println("Usages: [hostname] [lock name]");
			System.exit(0);
		}

		DistLock lock = new DistLock();
		// Not-so-important: just a fast disconnection...
		Runtime.getRuntime().addShutdownHook(new DistLockShutdownThread(lock));

		lock.connect(args[0]);
		lock.setName(args[1]);
		lock.get();

		System.out.println("lock taken!");

		Thread.sleep(Long.MAX_VALUE);		// dummy payload...
	}
}


class DistLockShutdownThread extends Thread
{
	private DistLock lock;

	public DistLockShutdownThread(DistLock lock)
	{
		this.lock = lock;
	}

	public void run()
	{
		try {
			this.lock.close();
		} catch(Exception e) {
			System.err.println(e);
		}
	}
}

