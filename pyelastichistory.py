from pyelasticsearch import ElasticHttpError, ElasticHttpNotFoundError
from pyelasticobjects import ObjectSearch
import time, hashlib, difflib, json

__author__ = 'Schuyler Erle'
__all__ = ['ElasticHistory']
__version__ = '0.1.0'
__version_info__ = tuple(__version__.split('.'))

class ElasticHistory(ObjectSearch):
    def _history_index(self, index):
        return index + "-history"

    def _revision_type(self):
        return "revision"

    def index(self, index, doc_type, doc, id=None,
            force_insert=False, metadata={}):
        history_index = self._history_index(index)
        
        #See if there is a previous version
        last_doc = None
        try:
            last_doc = super(ElasticHistory, self).get(index, doc_type, id)
        except ElasticHttpNotFoundError:
            pass
        
        #store the new doc
        result = super(ElasticHistory, self).index(
                 index, doc_type, doc, id, force_insert)
        id = result.id

        #get the history from the brand new doc, or from existing one
        history = {
            "index": index,
            "type": doc_type,
            "id": id,
            "revisions": []
        }
        try:
            history = self.get(history_index, doc_type, id)
        except ElasticHttpError:  #possibly ElasticHttpNotFoundError?
            pass
        # FIXME: remove this when pyelasticsearch uses self.from_python internally
        converted = self.from_python(doc)
        digest = hashlib.sha1(json.dumps(converted, sort_keys=True)).hexdigest()
        metadata = metadata.copy()
        metadata["created_at"] = time.time()
        metadata["digest"] = digest
        history["revisions"].append(metadata)
        #write the history for the doc
        self._write_history(index, doc_type, history,  id)

        #store the new doc as a revisions in the history index
        #only store a doc if one was there previously
        if last_doc:
            converted_last = self.from_python(last_doc)
            last_digest = history["revisions"][len(history["revisions"])-2]["digest"]

            super(ElasticHistory, self).index(
                    history_index, self._revision_type(), converted_last, last_digest)
        return result

    def _write_history(self, index, doc_type, history, id):
        # FIXME: remove this when pyelasticsearch uses self.from_python internally
        converted = self.from_python(history)
        super(ElasticHistory, self).index( 
                 self._history_index(index), doc_type, converted, id)
        
    def history(self, index, doc_type, id):
        return self.get(self._history_index(index), doc_type, id)

    def revision(self, index, doc_type, id, digest):
        history = self.history(index, doc_type, id)
        revisions = history["revisions"]
        latest_digest = revisions[len(revisions)-1]["digest"]
        if latest_digest == digest:
            #Get the revision from the original index, its the same.
            return self.get(index, doc_type, id)
        #else get revision from history
        return self.get(self._history_index(index),
                self._revision_type(), digest)

    def delta(self, index, doc_type, id, digest1, digest2):
        doc1 = self.from_python( self.revision(index, doc_type, id, digest1) )
        doc2 = self.from_python( self.revision(index, doc_type, id, digest2) )
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
        if branch_point is None:
            raise ValueError("Document does not have revision %s" % revision)
        if branch_point == len(revisions) - 1:
            raise ValueError("Cannot rollback to the current revision")
        previous_version = self.revision(index, doc_type, id, revision)
        # FIXME: remove this when pyelasticsearch uses self.from_python internally
        converted = self.from_python(previous_version)
        self.index(index, doc_type, converted, id)
