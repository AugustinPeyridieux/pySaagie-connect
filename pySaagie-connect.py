from hdfs import InsecureClient
import ibis
import requests
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
import urllib.parse
import pandas as pd
import json
from pandas.io.json import json_normalize


def get_url_active_namenode(list_name_nodes):
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
        hadoop = InsecureClient(url)
        try:
            # Test if the HDFS client is working
            hadoop.status('/')
            return url
        except:
            # if an error occure, it means the NameNode used to connect to HDFS is not active
            if cpt == len(list_name_nodes):
                # if we tested both NameNodes, it means there is no active NameNode
                # HDFS is not reachable
                raise NameError("No NameNode available")
            else:
                cpt += 1

def return_client_hdfs(user):
    """
    Connect to HDFS with the active NameNode

    :param user: string user to connect to HDFS
    :return: client HDFS with an active NameNode
    """

    url = get_url_active_namenode()
    client_hdfs = InsecureClient(url, user=user)
    return client_hdfs

def get_client_ibis_impala(user,
                           user_password,
                           list_datanodes,
                           port_impala = 21050,
                           port_hdfs = 50070,
                           impala_ssl = 0):
    """

    :param user: string of the username to connect to Impala
    :param user_password:  string password to connect to Impala
    :param list_datanodes: List containing the list of dataNode
    :param port_impala: port Impala to use (default 21050)
    :param port_hdfs: port HDFS to use (default 50070)
    :param impala_ssl: set to 1 if you want to activate SSL on impala
    :return: ibis client with an active random datanode
    """

    # Get The active NameNode
    url_hdfs = get_url_active_namenode()

    # Use the URL of the active NameNode to create an IBIS HDFS client
    ibis_hdfs = ibis.hdfs_connect(host=url_hdfs, port=int(port_hdfs))

    # Create a list of available DataNodes
    data_node_list = list_datanodes.split(';')

    # Loop through all Data Nodes to test if they are active or not
    while len(data_node_list) > 0:
        # Randomly select one the DataNode
        data_node = data_node_list[random.randint(0, len(data_node_list) - 1)]
        try:
            # Test if the DataNode is active
            ibis_client = ibis.impala.connect(host=data_node, port=int(port_impala),
                                              hdfs_client=ibis_hdfs,
                                              user=user,
                                              password=user_password,
                                              auth_mechanism='PLAIN',
                                              use_ssl=bool(impala_ssl == "1"),
                                              timeout=0.5)
            ibis_client.list_databases()
            return ibis_client

        except:
            # If an error occure, it means the DataNode is not ative or does not exists
            # -> We need to remove that DataNode from our list to avoir infinite loop and testing a DataNode twice
            data_node_list.remove(data_node)

    raise NameError("No DataNode available for Impala")
