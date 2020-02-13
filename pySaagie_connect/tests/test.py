from pySaagie_connect import pySaagie_connect as sc

client_hdfs = sc.return_client_hdfs(user='auser_name'
                                    , list_name_nodes=['http://nn1', 'http://nn2']
                                    , port_hdfs=50070)
print(client_hdfs.status('/'))

client_impala = sc.return_ibis_client('user_name'
                      , 'password'
                      , ['dn1', 'dn2', 'dn3', 'dn4']
                      , ['http://nn1', 'http://nn2']
                      , 21050
                      , 50070)
print(client_impala.exists_database('default'))