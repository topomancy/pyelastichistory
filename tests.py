"""
Unit tests for pyelastichistory.  These require an elasticsearch server running
on the default port (localhost:9200).
"""
import unittest, time
from pyelastichistory import ElasticHistory, json
from pyelasticsearch import *
# copied with impunity from pyelasticsearch's test.py
#
class ElasticHistoryTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = ElasticHistory('http://localhost:9200/')

    def tearDown(self):
        try:
            self.conn.delete_index("test-index")
            self.conn.delete_index("test-index-history")
        except:
            pass

    def assertResultContains(self, result, expected):
        for (key, value) in expected.items():
            self.assertEquals(value, getattr(result, key))

    def assertDocumentContains(self, result, expected):
        for (key, value) in expected.items():
            self.assertEquals(value, result[key])
  

class IndexingTestCase(ElasticHistoryTestCase):
    def testIndexingWithID(self):
        start = time.time()
        result = self.conn.index("test-index", "test-type", {"name": "Joe Tester"},1, metadata={"user_created": "Jane Editor"})
        self.assertResultContains(result, {'type': 'test-type', 'id': '1',
            'ok': True, 'index': 'test-index' })

        history = self.conn.history("test-index", "test-type", 1)
        self.assertDocumentContains(history, {
            "index": "test-index",
            "type": "test-type",
            "id": "1"
        })
        self.assertTrue("revisions" in history)
        self.assertEqual(len(history["revisions"]), 1)

        metadata = history["revisions"][0]
        self.assertTrue(metadata["created_at"] > start)
        self.assertTrue(len(metadata["digest"]) == 40) # SHA-1
        self.assertEqual(metadata["user_created"], "Jane Editor")

        revision = self.conn.revision("test-index", "test-type", "1", metadata["digest"])
        self.assertDocumentContains(revision, {"name": "Joe Tester"})

    def testIndexingWithoutID(self):
        result = self.conn.index("test-index", "test-type", {"name": "Joe Tester"},1, metadata={"user_created": "Jane Editor"})
        self.assertResultContains(result, {'type': 'test-type',
            'ok': True, 'index': 'test-index' })
        id = result.id

        history = self.conn.history("test-index", "test-type", id)
        self.assertDocumentContains(history, {
            "index": "test-index",
            "type": "test-type",
            "id": id
        })

        self.assertEqual(len(history["revisions"]), 1)
        metadata = history["revisions"][0]
        self.assertEqual(metadata["user_created"], "Jane Editor")
        revision = self.conn.revision("test-index", "test-type", id, metadata["digest"])
        self.assertDocumentContains(revision, {"name": "Joe Tester"})

     
    
    def testRevisionTracking(self):
        self.conn.index("test-index", "test-type", {"name": "Joe Tester"},1, metadata={"user_created": "Jane Editor"})
        self.conn.index("test-index", "test-type", {"name": "Joe Q. Tester"},1, metadata={"user_created": "Jane J. Editor"})
        history = self.conn.history("test-index", "test-type", 1)
        
        self.assertEqual(len(history["revisions"]), 2)

        meta1, meta2 = history["revisions"]
        self.assertTrue(meta1["created_at"] < meta2["created_at"])
        self.assertNotEqual(meta1["digest"],  meta2["digest"])
        self.assertEqual(meta1["user_created"], "Jane Editor")
        self.assertEqual(meta2["user_created"], "Jane J. Editor")

        revision = self.conn.revision("test-index", "test-type", "1", meta2["digest"])
        self.assertDocumentContains(revision, {"name": "Joe Q. Tester"})

    def testRollback(self):
        self.conn.index("test-index", "test-type", {"name": "Joe Tester"},1)
        self.conn.index("test-index", "test-type", {"name": "Joe Q. Tester"},1)
        self.conn.index("test-index", "test-type", {"name": "Joe Q. Tester, Esq."},1)
        current = self.conn.get("test-index", "test-type", 1)
        self.assertEqual(current["name"], "Joe Q. Tester, Esq.")

        history = self.conn.history("test-index", "test-type", 1)
        self.conn.rollback("test-index", "test-type", 1, history["revisions"][0]["digest"])
        current = self.conn.get("test-index", "test-type", 1)
        self.assertEqual(current["name"], "Joe Tester")

        history = self.conn.history("test-index", "test-type", 1)
        self.assertEqual(len(history["revisions"]), 4)
        self.assertEqual(history["revisions"][0]["digest"],
                         history["revisions"][-1]["digest"])

class IndexingTestCaseStorage(ElasticHistoryTestCase):

    def testRevisionStorage(self):
        first_doc = {"name": "Joe Tester"}
        self.conn.index("test-index", "test-type", first_doc,1, metadata={"user_created": "Jane Editor"})
        self.conn.refresh(["test-index"])
        history = self.conn.history("test-index", "test-type", 1)
        
        #there should only be 1 entry in the history, as expected
        self.assertEqual(len(history["revisions"]), 1 )
        
        #get the digest for this first, and only entry
        first_digest = history["revisions"][0]["digest"]
        
        #This first digest should not be saved as a revision
        #it should give a not found error when we go and get it ElasticHttpNotFoundError
        
        #FIXME = sometimes this erroneously raises: NoShardAvailableActionException
        #ElasticHttpError: (500, u'NoShardAvailableActionException[[test-index-history][3] No shard available for [[test-index-history][revision][1e5f73effc4403c27a193da38d225be27efc94f3]: routing [null]]]'
        self.assertRaises(ElasticHttpError, self.conn.get, "test-index-history", "revision", first_digest)
        
        #it may not be stored in history but that the document can be got via revision method
        revision = self.conn.revision("test-index", "test-type", "1", first_digest)
        self.assertDocumentContains(revision, first_doc)

        #lets add another doc
        self.conn.index("test-index", "test-type", {"name": "Joe Q. Tester"},1, metadata={"user_created": "Jane J. Editor"})     
        
        #lets add another one - this one will now be the last 
        self.conn.index("test-index", "test-type", {"name": "Last Joe Q. Tester"},1, metadata={"user_created": "Jane J. Editor"})     
        
        self.conn.refresh(["test-index"])
        
        #Get the first doc. No error now, as it has been stored as a revision
        first_doc =  self.conn.get("test-index-history", "revision", first_digest)
        
        #Is the stored revision the same as the first doc?
        self.assertEqual(first_doc.source, first_doc)
        
        #lets get this first doc via revision method too
        first_revision = self.conn.revision("test-index", "test-type", "1", first_digest)
        self.assertDocumentContains(first_revision, first_doc)
        
        new_history = self.conn.history("test-index", "test-type", 1)
        self.assertEqual(len(new_history["revisions"]), 3 ) 

        last_digest = new_history["revisions"][2]["digest"]
        #the last doc should not be stored
        self.assertRaises(ElasticHttpError, self.conn.get, "test-index-history", "revision", last_digest)
 
        #check that the last one is got, and matches up anyhow.
        current = self.conn.get("test-index", "test-type", 1)
        self.assertEqual(current.source, {"name": "Last Joe Q. Tester"})

if __name__ == "__main__":
    unittest.main()

