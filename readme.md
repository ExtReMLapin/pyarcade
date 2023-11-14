Based on https://github.com/adaros92/arcadedb-python-driver but reworked and with bugfixes


`SyncClient` `object` imported from pyarcade.api.sync acts like an ID card, it stores your password and login to the ArcadeDB server
`DatabaseDao` `class` acts like a tool to create or delete databases (static methods only)
`DatabaseDao` `object` (when instancied with a client and db name as params) allows to control a specific database, run queries


### Client instanciation : 

```python
from pyarcade.api.sync import SyncClient
syncclient = SyncClient("192.168.25.25", 2480,  username="root", password="eh eh password", content_type="application/json")
```


### Toying with database
`from pyarcade.dao.database import DatabaseDao`

#### Create a database : 

```python
db_created = DatabaseDao.create(syncclient, "Test")
print(db_created)
```
`<DatabaseDao database_name=Testdfgdfg> @ <host=192.168.25.25 port=2480 user=root>`

#### Drop a database : 


```python
DatabaseDao.delete(syncclient, "Test66566")
```


#### Run a query

```python
qer_result = db_created.query("cypher", "match (n) return n", limit=3)
print(qer_result)

```

>[{'@rid': '#1:15', '@type': 'Movie', 'title': 'Jerry Maguire', 'released': 2000, 'tagline': 'The rest of his life begins now.'}, {'@rid': '#1:16', '@type': 'Movie', 'title': 'When Harry Met Sally', 'released': 1998, 'tagline': 'Can two friends sleep together and still love each other in the morning?'}, {'@rid': '#1:17', '@type': 'Movie', 'title': 'The Da Vinci Code', 'released': 2006, 'tagline': 'Break The Codes'}, {'@rid': '#1:18', '@type': 'Movie', 'title': 'Twister', 'released': 1996, 'tagline': "Don't Breathe. Don't Look Back."}]

#### List databases

```python
print(DatabaseDao.list_databases(syncclient))
```

>['Test6656', 'Test', 'Testdfgdfg', 'Test666', 'Test42', 'Test665', 'Test656566', 'Test656566dada', 'Test66', 'Test2', 'Test66566']
