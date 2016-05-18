import java.util.List;

//import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;
//import org.apache.zookeeper.CreateMode;
//import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.*;
//import org.apache.zookeeper.Watcher.Event.EventType;

public class DeleteGroup implements Watcher
{
	private String groupName = null;
	private ZooKeeper zk = null;
	private boolean done = false;

	/**
	 * Following OO-design: set the group path name.
	 */
	public void setName(String name)
	{
		this.groupName = name;
	}

	/**
	 * Create the group and go away
	 */
	public void delete() throws KeeperException, InterruptedException
	{
		String path = "/" + groupName;

		try
		{
			List<String> children = zk.getChildren(path, false);
			for (String child : children) {
				zk.delete(path + "/" + child, -1);
				System.out.println("Znode deleted: " + path + "/" + child);
			}
			zk.delete(path, -1);
			System.out.println("Znode deleted: " + path);
		}
		catch(KeeperException.NodeExistsException e)
		{
			System.out.printf("Group %s exist, bye!\n", groupName);
			System.exit(0);
		}
	}

	public void process(WatchedEvent e)
	{
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
			System.err.println("Usages: [hostname] [group name]");
			System.exit(0);
		}

		DeleteGroup g = new DeleteGroup();
		// Not-so-important: just a fast disconnection...

		g.connect(args[0]);
		g.setName(args[1]);
		g.delete();
		g.close();
	}
}
