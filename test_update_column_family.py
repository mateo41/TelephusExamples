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
    #Add 3 rows to the column family 
    yield client.insert('bsanderson', CF, 'Brandon Sanderson', column='full_name') 
    yield client.insert('bsanderson', CF, '1975', column='birth_date') 
    yield client.insert('prothfuss', CF, 'Patrick Rothfuss', column='full_name') 
    yield client.insert('prothfuss', CF, '1973', column='birth_date') 
    yield client.insert('htayler', CF, 'Howard Tayler', column='full_name') 
    yield client.insert('htayler', CF, '1968', column='birth_date') 
    
    yield client.insert('bsanderson', CF, 'UT', column='state') 
    yield client.insert('prothfuss', CF, 'WI', column='state') 
    yield client.insert('htayler', CF, 'UT', column='state') 
    
    #Search using the index by the full_name
    selection_predicate1 = [IndexExpression('full_name', IndexOperator.EQ, 'Howard Tayler')]
    res = yield client.get_indexed_slices(CF, selection_predicate1)
    print res 
    
    #Search using the index by the birth_date 
    selection_predicate2 = [IndexExpression('birth_date', IndexOperator.EQ, '1968')] 
    res = yield client.get_indexed_slices(CF, selection_predicate2)
    print res 
    
    #Search using the index by the birth_date and full_name 
    #I don't know if it's possible to use OR and NOT. 
    selection_predicate3 = [IndexExpression('birth_date', IndexOperator.EQ, '1968'), IndexExpression('full_name', IndexOperator.EQ, 'Howard Tayler')] 
    res = yield client.get_indexed_slices(CF, selection_predicate3)
    print res 
    
    #Trying to update the column family to add another Index for state 
    #I'm not sure how to do this. I'll ask the Telephus guys 
    cf_def = CfDef(keyspace=KEYSPACE, 
                   name = CF,
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
    """ 
    selection_predicate1 = [IndexExpression('state', IndexOperator.EQ, 'UT')]
    res = yield client.get_indexed_slices(CF, selection_predicate1)
    print res 
    selection_predicate2 = [IndexExpression('birth_date', IndexOperator.EQ, '1973')]
    res = yield client.get_indexed_slices(CF, selection_predicate2)
    print res 
    """
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
