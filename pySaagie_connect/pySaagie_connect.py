from hdfs import InsecureClient
import ibis
import random


def get_url_active_namenode(list_name_nodes, port_hdfs):
    """
    Return the URL of the active NameNode
    Context:
    With High availability in HDFS, 2 NameNode are up on the cluster
    If one NameNode is down the other NameNode et catching up and start doing the job.
    It means that the active NameNode is not always the same and to be sure to connect to HDFS
    you need to test if you are connecting to HDFS with the active NameNode.
    This function is accomplishing this purpose

    :param list_name_nodes: List of Name Node (e.g.: ['http://nn2:50070', 'http://nn1:50070'])
    :return: string URL of the active NameNode
    """

    cpt = 1
    # Loop to identify which one of the 2 NameNode is active
    for url in list_name_nodes:
        # Create the HDFS client
        hadoop = InsecureClient(url + ':' + port_hdfs)
        try:
            # Test if the HDFS client is working
            hadoop.status('/')
            return url + ':' + port_hdfs
        except:
            # if an error occure, it means the NameNode used to connect to HDFS is not active
            if cpt == len(list_name_nodes):
                # if we tested both NameNodes, it means there is no active NameNode
                # HDFS is not reachable
                raise NameError("No NameNode available")
            else:
                cpt += 1


def return_client_hdfs(user, list_name_nodes, port_hdfs):
    """
    Connect to HDFS with the active NameNode

    :param user: string user to connect to HDFS
    :return: client HDFS with an active NameNode
    """

    url = get_url_active_namenode(list_name_nodes, port_hdfs)
    client_hdfs = InsecureClient(url, user=user)
    return client_hdfs


def return_ibis_client(user
                       , user_password
                       , list_datanodes
                       , list_name_nodes
                       , port
                       , port_hdfs = 50070
                       , auth_mechanism = 'PLAIN'
                       , ssl = 0):
    """
    Return an ibis client for Hive or Impala
    :param user: string of the username to connect to Impala
    :param user_password:  string password to connect to Impala
    :param list_datanodes: List containing the list of dataNode
    :param port: port Impala or Hive to use (default 21050 for Impala and 10000 for Hive)
    :param port_hdfs: port HDFS to use (default 50070)
    :param impala_ssl: set to 1 if you want to activate SSL on impala
    :return: ibis client with an active random datanode
    """

    # Get The active NameNode
    url_hdfs = get_url_active_namenode(list_name_nodes, port_hdfs)

    # Use the URL of the active NameNode to create an IBIS HDFS client
    ibis_hdfs = ibis.hdfs_connect(host=url_hdfs, port=int(port_hdfs))

    # Loop through all Data Nodes to test if they are active or not
    while len(list_datanodes) > 0:
        # Randomly select one the DataNode
        data_node = list_datanodes[random.randint(0, len(list_datanodes) - 1)]
        try:
            # Test if the DataNode is active
            ibis_client = ibis.impala.connect(host=data_node, port=int(port),
                                              hdfs_client=ibis_hdfs,
                                              user=user,
                                              password=user_password,
                                              auth_mechanism=auth_mechanism,
                                              use_ssl=bool(ssl == "1"),
                                              timeout=0.5)
            ibis_client.list_databases()
            return ibis_client

        except:
            # If an error occure, it means the DataNode is not ative or does not exists
            # -> We need to remove that DataNode from our list to avoir infinite loop and testing a DataNode twice
            list_datanodes.remove(data_node)

    raise NameError("No DataNode available for Impala")