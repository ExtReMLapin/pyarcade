Based on https://github.com/adaros92/arcadedb-python-driver but reworked and with bugfixes


```python
from pyarcade.api.sync import SyncClient
from pyarcade.dao.database import DatabaseDao
syncclient = SyncClient("192.168.25.25", 2480,  username="root", password="eh eh password", content_type="application/json")

db_created = DatabaseDao.create(syncclient, "Test")
print(db_created)
```

><DatabaseDao database_name=Testdfgdfg> @ <host=192.168.25.25 port=2480 user=root>



```python
db = DatabaseDao(syncclient, "Test")
qer_result = db.query("cypher", "match (n) return n", limit=3)
print(qer_result)

```

>[{'@rid': '#1:15', '@type': 'Movie', 'title': 'Jerry Maguire', 'released': 2000, 'tagline': 'The rest of his life begins now.'}, {'@rid': '#1:16', '@type': 'Movie', 'title': 'When Harry Met Sally', 'released': 1998, 'tagline': 'Can two friends sleep together and still love each other in the morning?'}, {'@rid': '#1:17', '@type': 'Movie', 'title': 'The Da Vinci Code', 'released': 2006, 'tagline': 'Break The Codes'}, {'@rid': '#1:18', '@type': 'Movie', 'title': 'Twister', 'released': 1996, 'tagline': "Don't Breathe. Don't Look Back."}]


```python
print(DatabaseDao.list_databases(syncclient))
```

>['Test6656', 'Test', 'Testdfgdfg', 'Test666', 'Test42', 'Test665', 'Test656566', 'Test656566dada', 'Test66', 'Test2', 'Test66566']
