from pyelasticsearch import ElasticSearch, json
import time, hashlib, difflib

class ElasticHistory(ElasticSearch):
    def get_history_index(self, index):
        return index + "_history"

    def get_revision_type(self):
        return "revision"

    def index(self, doc, index, doc_type, id=None,
            force_insert=False, metadata={}):
        history_index = self.get_history_index(index)
        history = self.get(history_index, doc_type, id)
        if history is None:
            history = {
                "index": index
                "type": doc_type,
                "id": id,
                "revisions": []
            }
        digest = hashlib.sha1(json.dumps(doc, sort_keys=True)).hexdigest()
        metadata = metadata.copy()
        metadata["created_at"] = time.time()
        metadata["digest"] = digest
        history["revisions"].append(metadata)
        super(Waistband, self).index(
                doc, history_index, self.get_revision_type(), digest)
        super(Waistband, self).index(
                history, history_index, doc_type, id)
        super(Waistband, self).index(
                doc, index, doc_type, id, force_insert)
        
    def history(self, index, doc_type, id):
        return self.get(self.history_index(index), doc_type, id)

    def revision(self, index, digest):
        return self.get(self.history_index(index),
                self.get_revision_type(), digest)

    def delta(self, index, digest1, digest2):
        doc1 = self.revision(index, digest1)
        doc2 = self.revision(index, digest2)
        str1 = json.dumps(doc1, sort_keys=True, indent=4).splitlines(True)
        str2 = json.dumps(doc2, sort_keys=True, indent=4).splitlines(True)
        return difflib.unified_diff(str1, str2,
                fromfile=digest1, tofile=digest2)
