from pyelasticsearch import ElasticSearch, json
import time, hashlib, difflib

class ElasticHistory(ElasticSearch):
    def _history_index(self, index):
        return index + "_history"

    def _revision_type(self):
        return "revision"

    def index(self, doc, index, doc_type, id=None,
            force_insert=False, metadata={}):
        history_index = self._history_index(index)
        history = self.get(history_index, doc_type, id)
        if history is None: # FIXME?
            history = {
                "index": index
                "type": doc_type,
                "id": id,
                "revisions": [],
                "branches": {}
            }
        digest = hashlib.sha1(json.dumps(doc, sort_keys=True)).hexdigest()
        metadata = metadata.copy()
        metadata["created_at"] = time.time()
        metadata["digest"] = digest
        history["revisions"].append(metadata)
        self._write_history(history, index, doc_type, id)
        super(ElasticHistory, self).index(
                doc, history_index, self._revision_type(), digest)
        super(ElasticHistory, self).index(
                doc, index, doc_type, id, force_insert)

    def _write_history(self, history, index, doc_type, id):
        super(ElasticHistory, self).index( 
                history, self._history_index(index), doc_type, id)
        
    def history(self, index, doc_type, id):
        return self.get(self.history_index(index), doc_type, id)

    def revision(self, index, digest):
        return self.get(self.history_index(index),
                self._revision_type(), digest)

    def delta(self, index, digest1, digest2):
        doc2 = self.revision(index, digest2)
        str1 = json.dumps(doc1, sort_keys=True, indent=4).splitlines(True)
        str2 = json.dumps(doc2, sort_keys=True, indent=4).splitlines(True)
        return difflib.unified_diff(str1, str2,
                fromfile=digest1, tofile=digest2)

    def rollback(self, index, doc_type, id, revision):
        history = self.history(index, doc_type, id)
        revisions = history["revisions"]
        branch_point = None
        for i in range(len(revisions)):
            if revisions[i]["digest"] == revision:
                branch_point = i
                break
        branch = history["revisions"][branch_point+1:]
        if not branch:
            raise ValueError("Cannot rollback to the current revision")
        history["branches"].setdefault(revision, [])
        history["branches"][revision].append(branch)
        del history["revisions"][branch_point+1:]
        self._write_history(history, index, doc_type, id)
        self.index(self.revision(index, revision), index, doc_type, id)
