"""
Unit tests for pyelastichistory.  These require an elasticsearch server running
on the default port (localhost:9200).
"""
import unittest, time
from pyelastichistory import ElasticHistory, json

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
            self.assertEquals(value, result[key])

class IndexingTestCase(ElasticHistoryTestCase):
    def testIndexingWithID(self):
        start = time.time()
        result = self.conn.index({"name":"Joe Tester"}, "test-index", "test-type", 1,
                metadata={"user_created": "Jane Editor"})
        self.assertResultContains(result, {'_type': 'test-type', '_id': '1',
            'ok': True, '_index': 'test-index' })

        result = self.conn.history("test-index", "test-type", 1)
        history = result["_source"]
        self.assertResultContains(history, {
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

        revision = self.conn.revision("test-index", metadata["digest"])
        self.assertResultContains(revision["_source"], {"name": "Joe Tester"})

    def testIndexingWithoutID(self):
        result = self.conn.index({"name":"Joe Tester"}, "test-index", "test-type",
                metadata={"user_created": "Jane Editor"})
        self.assertResultContains(result, {'_type': 'test-type',
            'ok': True, '_index': 'test-index' })
        id = result["_id"]

        result = self.conn.history("test-index", "test-type", id)
        history = result["_source"]
        self.assertResultContains(history, {
            "index": "test-index",
            "type": "test-type",
            "id": id
        })

        self.assertEqual(len(history["revisions"]), 1)
        metadata = history["revisions"][0]
        self.assertEqual(metadata["user_created"], "Jane Editor")
        revision = self.conn.revision("test-index", metadata["digest"])
        self.assertResultContains(revision["_source"], {"name": "Joe Tester"})

    def testRevisionTracking(self):
        self.conn.index({"name":"Joe Tester"}, "test-index", "test-type", 1,
                metadata={"user_created": "Jane Editor"})
        self.conn.index({"name":"Joe Q. Tester"}, "test-index", "test-type", 1,
                metadata={"user_created": "Jane J. Editor"})
        history = self.conn.history("test-index", "test-type", 1)["_source"]
        self.assertEqual(len(history["revisions"]), 2)

        meta1, meta2 = history["revisions"]
        self.assertTrue(meta1["created_at"] < meta2["created_at"])
        self.assertNotEqual(meta1["digest"],  meta2["digest"])
        self.assertEqual(meta1["user_created"], "Jane Editor")
        self.assertEqual(meta2["user_created"], "Jane J. Editor")

        revision = self.conn.revision("test-index", meta2["digest"])
        self.assertResultContains(revision["_source"], {"name": "Joe Q. Tester"})

    def testRollback(self):
        self.conn.index({"name":"Joe Tester"}, "test-index", "test-type", 1)
        self.conn.index({"name":"Joe Q. Tester"}, "test-index", "test-type", 1)
        self.conn.index({"name":"Joe Q. Tester, Esq."}, "test-index", "test-type", 1)
        current = self.conn.get("test-index", "test-type", 1)
        self.assertEqual(current["_source"]["name"], "Joe Q. Tester, Esq.")

        history = self.conn.history("test-index", "test-type", 1)["_source"]
        self.conn.rollback("test-index", "test-type", 1, history["revisions"][0]["digest"])
        current = self.conn.get("test-index", "test-type", 1)
        self.assertEqual(current["_source"]["name"], "Joe Tester")

        history = self.conn.history("test-index", "test-type", 1)["_source"]
        self.assertEqual(len(history["revisions"]), 4)
        self.assertEqual(history["revisions"][0]["digest"],
                         history["revisions"][-1]["digest"])

if __name__ == "__main__":
    unittest.main()

