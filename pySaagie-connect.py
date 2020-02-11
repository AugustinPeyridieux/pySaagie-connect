from hdfs import InsecureClient
import ibis
import requests
from sqlalchemy import create_engine
from elasticsearch import Elasticsearch
import urllib.parse
import pandas as pd
import json
from pandas.io.json import json_normalize


def get_url_active_namenode():
    """
    Return the URL of the active NameNode
    Context:
    With High availability in HDFS, 2 NameNode are up on the cluster
    If one NameNode is down the other NameNode et catching up and start doing the job.
    It means that the active NameNode is not always the same and to be sure to connect to HDFS
    you need to test if you are connecting to HDFS with the active NameNode.
    This function is accomplishing this purpose
    # Environement variables examples (to adapt according to the configuration of your platform)
    # os.environ['URL_NN1'] = 'http://nn1'
    # os.environ['URL_NN2'] = 'http://nn2'
    # os.environ['PORT_HDFS'] = '50070'
    """
    # Create a list of Urls containing the 2 NameNodes
    HDFS_URLS = [os.environ['URL_NN1'] + ':' + os.environ['PORT_HDFS'],
                 os.environ['URL_NN2'] + ':' + os.environ['PORT_HDFS']]
    cpt = 1
    # Loop to identify which one of the 2 NameNode is active
    for url in HDFS_URLS:
        # Create the HDFS client
        hadoop = InsecureClient(url)
        try:
            # Test if the HDFS client is working
            hadoop.status('/')
            return url
        except:
            # if an error occure, it means the NameNode used to connect to HDFS is not active
            if cpt == len(HDFS_URLS):
                # if we tested both NameNodes, it means there is no active NameNode
                # HDFS is not reachable
                raise NameError("No NameNode available")
            else:
                cpt += 1

def return_client_hdfs(user):
    """
    Return an HDFS Client for a specific user
    """
    url = get_url_active_namenode(user)
    client_hdfs = InsecureClient(url, user=user)
    return client_hdfs

def get_client_ibis_impala(user, user_password):
    """
    Get an ibis impala client with an active random DataNode from the list of 
    DataNodes in the the environment variable 'LIST_DATANODES'
    
    # Environement variables examples (to adapt according to the configuration of your platform)
    # os.environ['LIST_DATANODES'] =  'dn1;dn2;dn3;dn4;dn5;dn6;dn7;dn8;dn9'
    # os.environ['PORT_HDFS'] = 50070
    # os.environ['PORT_IMPALA'] = 21050
    # os.environ['IMPALA_SSL_ACTIVATED'] -> 1 to activate SSL
    """
    # Get The active NameNode
    url_hdfs = get_url_active_namenode(user)

    # Use the URL of the active NameNode to create an IBIS HDFS client
    ibis_hdfs = ibis.hdfs_connect(host=url_hdfs, port=int(os.environ['PORT_HDFS']))

    # Create a list of available DataNodes
    data_node_list = os.environ['LIST_DATANODES'].split(';')

    # Loop through all Data Nodes to test if they are active or not
    while len(data_node_list) > 0:
        # Randomly select one the DataNode
        data_node = data_node_list[random.randint(0, len(data_node_list) - 1)]
        try:
            # Test if the DataNode is active
            ibis_client = ibis.impala.connect(host=data_node, port=int(os.environ['PORT_IMPALA']),
                                              hdfs_client=ibis_hdfs,
                                              user=user,
                                              password=user_password,
                                              auth_mechanism='PLAIN',
                                              use_ssl=bool(os.environ['IMPALA_SSL_ACTIVATED'] == "1"),
                                              timeout=0.5)
            ibis_client.list_databases()
            return ibis_client

        except:
            # If an error occure, it means the DataNode is not ative or does not exists
            # -> We need to remove that DataNode from our list to avoir infinite loop and testing a DataNode twice
            data_node_list.remove(data_node)

    raise NameError("No DataNode available for Impala")

def hdfs_connect(hdfsip, user):
    """
    Connexion to the hdfs - uses hdfsCLI
    :return: client_hdfs -
    """
    # Connecting to Webhdfs by providing hdfs host ip and webhdfs port (50070 by default)
    client_hdfs = InsecureClient('http://' + hdfsip + ':50070', user=user)

    print('Connected to hdfs')
    return client_hdfs
