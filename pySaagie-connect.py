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


def impala_connect(hdfsip, impalaip, user, pwd):
    """
    Connexion to impala - uses ibis
    :param hdfsip:
    :param impalaip:
    :param user:
    :param pwd:
    :return: client
    """
    # Connecting to Impala by providing Impala host ip and port (21050 by default),credentials and a Webhdfs client
    hdfs = ibis.hdfs_connect(host=hdfsip, port=50070)
    client = ibis.impala.connect(host=impalaip, port=21050, hdfs_client=hdfs, user=user,
                                 password=pwd, auth_mechanism='PLAIN')
    print('Connected to impala')
    return client


def hive_connect(hdfsip, hiveip, user, pwd):
    """
    Connexion to hive - uses ibis
    :param hdfsip:
    :param hiveip:
    :param user:
    :param pwd:
    :return: client
    """
    # ====== Ibis conf (to avoid a bug) ======
    with ibis.config.config_prefix('impala'):
        ibis.config.set_option('temp_db', '`__ibis_tmp`')

    # Connecting to Impala by providing Impala host ip and port (21050 by default),credentials and a Webhdfs client
    hdfs = ibis.hdfs_connect(host=hdfsip, port=50070)
    client = ibis.impala.connect(host=hiveip, port=10000, hdfs_client=hdfs, user=user,
                                 password=pwd, auth_mechanism='PLAIN')
    print('Connected to hive')
    return client


def mysql_connect(user, pwd,sqlip, sqlport):
    """
    Connection to MySQL, uses mysql connector and sqlachelchemy
    Beware, use pip install mysql-connector==2.1.4
    :param user:
    :param pwd:
    :param sqlip:
    :param sqlport:
    :return:
    """

    # ====== Connection ====== #
    # Connecting to mysql by providing a sqlachemy engine
    engine = create_engine('mysql+mysqlconnector://'+user+':'+pwd+'@'+sqlip+':'+sqlport+'/sandbox', echo=False)

    return engine


def postgresql_connect(user,pwd,postgreip,postgreport, db):
    """
    Connection to postgreSQL, uses psycopg2 and sqlalchemy
    :param user:
    :param pwd:
    :param postgreip:
    :param postgreport:
    :param db:
    :return:
    """

    # ====== Connection ======
    # Connecting to PostgreSQL by providing a sqlachemy engine
    engine = create_engine('postgresql://'+user+':'+pwd+'@'+postgreip+':'+postgreport+'/'+db, echo=False)

    return engine


def elastic_connect(elastip, elastiport):
    """
    Connection to Elasticsearch, uses elasticsearch
    :param elastip:
    :param elastiport:
    :return:
    """

    # ====== Connection ====== #
    # Connection to ElasticSearch
    es = Elasticsearch(['http://' + elastip + ':' + elastiport],timeout=600)
    return es


class SharepointApi:
    """
    Classe pour requêter l'API sharepoint
    
    TODO: Faire la gestion de l'expiration de token
    TODO: Compléter les requêtes
    TODO: faire la gestion des erreurs
    """
    
    def __init__(self, host, site, client_secret, client_id, tenant_id, url_access="", principal=""):
        # Déclaration des variables sharepoint
        # Nom de domaine
        self.host = host
        # Nom du domaine pour récupérer le token (sera toujours le même)
        if not url_access:
            self.url_access = "accounts.accesscontrol.windows.net/"
        else:
            self.url_access = url_access
            
        # Nom de la team / site / autre où les fichiers ou autres seront stockés (visible dans l'url quand dans le dossier sharepoint)
        self.site = site
        
        # Client secret à demander à l'administrateur Office365
        self.client_secret = client_secret
        
        # Client ID à demander à l'administrateur Office365
        self.client_id = client_id
        
        # Tenant id à demander à l'administrateur Office365
        self.tenant_id = tenant_id
        
        # Principal qui ne change pas (à vérifier)
        if not principal:
            self.principal = "00000003-0000-0ff1-ce00-000000000000"
        else:
            self.principal = principal
            
        # Url principale
        self.api_url = "https://" + self.host + "/teams/"+ self.site + "/_api/"
        
        # Url de demande de token
        self.api_url_token = "https://" + self.url_access + self.tenant_id + "/tokens/OAuth/2"
        
        # Token d'identification
        self.get_token()
    
    def get_token(self):
        payload = "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data;" + \
            "name=\"grant_type\"\r\n\r\nclient_credentials\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition:" + \
            "form-data; name=\"client_id\"\r\n\r\n" + self.client_id + "@" + self.tenant_id + "\r\n" + \
            "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data;" + \
            "name=\"client_secret\"\r\n\r\n"+ self.client_secret + "\r\n" + \
            "------WebKitFormBoundary7MA4YWxkTrZu0gW\r\nContent-Disposition: form-data;" + \
            "name=\"resource\"\r\n\r\n" + self.principal + "/" + self.host + \
            "@" + self.tenant_id + "\r\n------WebKitFormBoundary7MA4YWxkTrZu0gW--"
        headers = {
            'content-type': "multipart/form-data; boundary=----WebKitFormBoundary7MA4YWxkTrZu0gW",
            'cache-control': "no-cache"
            }
        response = requests.request("POST", self.api_url_token, data=payload, headers=headers)
        self.token = response.json()['access_token']
        return 1  # TODO a améliorer

    def get_list_files_from_folder(self, path_folder):  
        path = urllib.parse.quote("web/GetFolderByServerRelativeUrl('" + path_folder + "')/Files", safe='')

        url = self.api_url + path

        headers = {
                'Authorization': "Bearer " + self.token,
                'Accept': "application/json;odata=nometadata",
                'cache-control': "no-cache",
            }

        return requests.request("GET", url, headers=headers).json()
    
    def downloadFileFromSharepoint(self, foldername, filename):
        path = urllib.parse.quote("web/GetFolderByServerRelativeUrl('" + foldername + "')", safe='')

        file_url = urllib.parse.quote("/Files('" + filename + "')/$value", safe = '')

        url = self.api_url + path + file_url

        headers = {
                'Authorization': "Bearer " + self.token,
                'Accept': "application/json;odata=nometadata",
                'cache-control': "no-cache",
            }

        r = requests.request("GET", url, headers=headers, stream=True)
        with open(filename, 'wb+') as f:
            for chunk in r.iter_content(chunk_size=1024): 
                if chunk:
                    f.write(chunk)
                    f.flush()
        return 1 # TODO: A améliorer

    def get_metadata_list_sharepoint(self, list_name):
        # http://server/site/_api/lists/getbytitle('listname')
        path = urllib.parse.quote("lists/getbytitle('" + list_name + "')", safe='')
        url = self.api_url + path
        headers = {
                'Authorization': "Bearer " + self.token,
                'Accept': "application/json;odata=nometadata",
                'cache-control': "no-cache",
            }

        r = requests.request("GET", url, headers=headers, stream=True)
        
        return r # TODO: A améliorer
    
    def get_list_sharepoint(self, list_name):
        r = self.get_metadata_list_sharepoint(list_name)
        print('List size: ' + str(r.json()['ItemCount']) + ' rows')
        print('Depending the charge on server side it will take approximately ' +\
              str(int(int(r.json()['ItemCount'] * 0.005666666666666667) / 60)) + 'min to pull the full list')
        path = urllib.parse.quote("lists/getbytitle('" + list_name + "')/items", safe='') + "?$skiptoken=Paged%3dTRUE"
        url = self.api_url + path
        headers = {
                'Authorization': "Bearer " + self.token,
                'Accept': "application/json;odata=nometadata",
                'cache-control': "no-cache",
            }


        to_continue = True
        df = pd.DataFrame()
        # When the list is empty return a dataFrame empty
        if r.json()['ItemCount'] == 0:
            return df
        while to_continue:
            r = requests.request("GET", url, headers=headers, stream=True)
            data = json.loads(r.text)
            df = df.append(json_normalize(data['value']))

            try:
                url = r.json()['odata.nextLink']
            except:
                to_continue = False
        return df

    def get_all_lists(self):
        # Get all lists of sharepoints
        path = "lists"
        url = self.api_url + path
        headers = {
                'Authorization': "Bearer " + self.token,
                'Accept': "application/json;odata=nometadata",
                'cache-control': "no-cache",
        }
        r = requests.request("GET", url, headers=headers, stream=True)
        return r
