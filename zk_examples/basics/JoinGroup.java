//import java.util.List;

import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.CreateMode;
//import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.*;
//import org.apache.zookeeper.Watcher.Event.EventType;

public class JoinGroup implements Watcher
{
	private String groupName = null;
	private String memberName = null;
	private String memberType = null;
	private ZooKeeper zk = null;
	private boolean done = false;
	//private Integer inLock = null;

	/**
	 * Constructor
	 */
	public JoinGroup()
	{
		//inLock = new Integer(0);
	}

	/**
	 * Following OO-design: set the member type.
	 */
	public void setMemberType(String name)
	{
		if(!name.equals("Normal") && !name.equals("Sequential"))
		{
			System.out.println("Member type is either 'Normal' or 'Sequential'. Bye\n");
			System.exit(1);
		}

		this.memberType = name;
	}

	/**
	 * Following OO-design: set the group member name.
	 */
	public void setMemberName(String name)
	{
		this.memberName = name;
	}

	/**
	 * Following OO-design: set the group path name.
	 */
	public void setGroupName(String name)
	{
		this.groupName = name;
	}

	/**
	 * Create the group and go away
	 */
	public void create() throws KeeperException, InterruptedException
	{
		String path = "/" + groupName + "/" + memberName;

		try
		{
			String createdPath = null;
			if(this.memberType.equals("Normal"))
			{
				createdPath =
					zk.create(path,
						null /*data*/,
						Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL);
			}
			else
			{
				createdPath =
					zk.create(path,
						null /*data*/,
						Ids.OPEN_ACL_UNSAFE,
						CreateMode.EPHEMERAL_SEQUENTIAL);
			}

			System.out.println("Znode created: " + createdPath);
		}
		catch(KeeperException.NodeExistsException e)
		{
			System.out.printf("Group %s exist, bye!\n", path);
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
		if(args.length != 4)
		{
			System.err.println("Usages: [hostname] [group name] [member name] [type]");
			System.exit(0);
		}

		JoinGroup g = new JoinGroup();
		// Not-so-important: just a fast disconnection...

		g.connect(args[0]);
		g.setGroupName(args[1]);
		g.setMemberName(args[2]);
		g.setMemberType(args[3]);
		g.create();

		Thread.sleep(Long.MAX_VALUE);
	}
}
