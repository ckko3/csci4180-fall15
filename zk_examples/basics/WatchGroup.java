import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;

public class WatchGroup implements Watcher
{
	public String groupName = null;
	private ZooKeeper zk;

	public void setName(String name)
	{
		this.groupName = name;
	}

	public void list() throws KeeperException, InterruptedException
	{
		String path = "/" + groupName;

		try
		{
			List < String > children = zk.getChildren(path, true);
			if(children.isEmpty())
			{
				System.out.printf("No members in group %s\n", groupName);
			}
			for(String child:children)
			{
				System.out.println(child);
			}
		}
		catch(KeeperException.NoNodeException e)
		{
			System.out.printf("Group '%s' does not exist, bye!\n", groupName);
			System.exit(1);
		}
	}

	public void connect(String hosts)
	{
		try
		{
			zk = new ZooKeeper(hosts, 5000, this);
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
			zk.close();
		}
		catch(Exception e)
		{
			System.err.println(e);
		}
	}

	public void process(WatchedEvent event)
	{
		System.out.println("------------[" + event + "]");
		if(event.getType() == EventType.NodeChildrenChanged)
		{
			try
			{
				list();
			}
			catch(Exception e)
			{
				System.err.println(e);
			}
		}
		else if(event.getType() == EventType.NodeDeleted)
		{
			System.exit(0);
		}
	}

	public static void main(String[]args) throws Exception
	{
		if(args.length != 2)
		{
			System.err.println("Usage: [hostname] [group name]");
			System.exit(0);
		}

		WatchGroup watchGroup = new WatchGroup();
		watchGroup.connect(args[0]);
		watchGroup.setName(args[1]);
		watchGroup.list();

		Thread.sleep(Long.MAX_VALUE);
	}
}
