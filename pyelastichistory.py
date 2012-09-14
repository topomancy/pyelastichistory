from pyelasticsearch import ElasticSearch, ElasticSearchError, json
import time, hashlib, difflib

class ElasticHistory(ElasticSearch):
    def _history_index(self, index):
        return index + "-history"

    def _revision_type(self):
        return "revision"

    def index(self, doc, index, doc_type, id=None,
            force_insert=False, metadata={}):
        history_index = self._history_index(index)
        result = super(ElasticHistory, self).index(
                doc, index, doc_type, id, force_insert)
        id = result["_id"]
        history = {
            "index": index,
            "type": doc_type,
            "id": id,
            "revisions": []
        }
        try:
            history_result = self.get(history_index, doc_type, id)
            history = history_result["_source"]
        except ElasticSearchError:
            pass
        digest = hashlib.sha1(json.dumps(doc, sort_keys=True)).hexdigest()
        metadata = metadata.copy()
        metadata["created_at"] = time.time()
        metadata["digest"] = digest
        history["revisions"].append(metadata)
        self._write_history(history, index, doc_type, id)
        super(ElasticHistory, self).index(
                doc, history_index, self._revision_type(), digest)
        return result

    def _write_history(self, history, index, doc_type, id):
        super(ElasticHistory, self).index( 
                history, self._history_index(index), doc_type, id)
        
    def history(self, index, doc_type, id):
        return self.get(self._history_index(index), doc_type, id)

    def revision(self, index, digest):
        return self.get(self._history_index(index),
                self._revision_type(), digest)

    def delta(self, index, digest1, digest2):
        doc2 = self.revision(index, digest1)["_source"]
        doc2 = self.revision(index, digest2)["_source"]
        str1 = json.dumps(doc1, sort_keys=True, indent=4).splitlines(True)
        str2 = json.dumps(doc2, sort_keys=True, indent=4).splitlines(True)
        return difflib.unified_diff(str1, str2,
                fromfile=digest1, tofile=digest2)

    def rollback(self, index, doc_type, id, revision):
        history = self.history(index, doc_type, id)["_source"]
        revisions = history["revisions"]
        branch_point = None
        for i in range(len(revisions)):
            if revisions[i]["digest"] == revision:
                branch_point = i
                break
        if branch_point is None:
            raise ValueError("Document does not have revision %s" % revision)
        if branch_point == len(revisions) - 1:
            raise ValueError("Cannot rollback to the current revision")
        previous_version = self.revision(index, revision)["_source"]
        self.index(previous_version, index, doc_type, id)
