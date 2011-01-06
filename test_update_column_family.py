#!/usr/bin/python
from telephus.protocol import ManagedCassandraClientFactory
from telephus.client import CassandraClient
from telephus.cassandra.ttypes import ColumnPath, ColumnParent, Column, SuperColumn, KsDef, CfDef, ColumnDef
from telephus.cassandra.ttypes import IndexType, IndexExpression, IndexOperator
from twisted.internet import defer


#Set these appropriately
HOST = '10.162.55.85'
PORT = 9160

@defer.inlineCallbacks
def dostuff(client):
    KEYSPACE = "TestKeyspace"
    CF = "TestCF"
    try:
        yield client.system_drop_keyspace(KEYSPACE)
    except Exception, ex:
        print ex
    keyspace = KsDef(name=KEYSPACE,
                     strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                     replication_factor=1,
                     cf_defs =[ 
                              CfDef(keyspace=KEYSPACE, 
                                    name=CF,
                                    column_type='Standard',
                                    comparator_type='org.apache.cassandra.db.marshal.UTF8Type',
                                    column_metadata = [ 
                                    ColumnDef( 
                                        name='full_name', 
                                        validation_class = 'org.apache.cassandra.db.marshal.UTF8Type',  
                                        index_type=IndexType.KEYS,
                                        index_name='NameIndex'
                                              ),
                                    ColumnDef( 
                                        name='birth_date', 
                                        validation_class = 'org.apache.cassandra.db.marshal.IntegerType',  
                                        index_type=IndexType.KEYS,
                                        index_name='BDayIndex'
                                              )
                                     
                                        ]
                                    )
                               ]
                     )
        
    yield client.system_add_keyspace(keyspace)
    yield client.set_keyspace(KEYSPACE)
    desc = yield client.describe_keyspace(KEYSPACE) 
    print desc
    #This isn't the best way to get the id. This works because there is only one column family. It would be
    #better to retrieve the column family using a name.  
    cf_id = desc.cf_defs[0].id
     
    cf_def =  CfDef(keyspace=KEYSPACE, 
                   name = CF,
                   id = cf_id,
                   column_type = 'Standard',
                   comparator_type='org.apache.cassandra.db.marshal.UTF8Type',
                   column_metadata = [ ColumnDef(
                                       name='state',
                                       validation_class = 'org.apache.cassandra.db.marshal.UTF8Type',  
                                       index_type=IndexType.KEYS,
                                       index_name='StateIndex'
                                        ) 
                                     ]
                   )
     
                       
    #Uncomment to see the error.
    yield client.system_update_column_family(cf_def)
    yield client.system_drop_keyspace(KEYSPACE)
    
if __name__ == '__main__':
    from twisted.internet import reactor
    from twisted.python import log
    import sys
    log.startLogging(sys.stdout)

    f = ManagedCassandraClientFactory()
    c = CassandraClient(f)
    dostuff(c)
    reactor.connectTCP(HOST, PORT, f)
    reactor.run()
