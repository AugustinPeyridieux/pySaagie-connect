# pySaagie-connect
Connector description:  
Connect to HDFS with the active NameNode  
Connect to Hive or Impala with a random active DataNode  

## Installation
``` 
pip install git+https://github.com/saagie/pySaagie-connect.git
``` 

## Examples
##### HDFS
``` 
from pySaagie_connect import pySaagie_connect as sc

client_hdfs = sc.return_client_hdfs(user='auser_name'
                                    , list_name_nodes=['http://nn1', 'http://nn2']
                                    , port_hdfs=50070)
``` 

##### Impala or Hive (adapt the port)
``` 
from pySaagie_connect import pySaagie_connect as sc

client_impala = sc.return_ibis_client('user_name'
                      , 'password'
                      , ['dn1', 'dn2', 'dn3', 'dn4']
                      , ['http://nn1', 'http://nn2']
                      , 21050
                      , 50070)
``` 