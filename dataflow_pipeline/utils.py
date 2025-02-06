from google.cloud import firestore

def fetch_config_from_firestore(config_document_path):
    """Fetch config from Firestore given a document path like 'scrubFilesConfig/<docID>'."""
    db = firestore.Client()
    doc_ref = db.document(config_document_path)
    doc = doc_ref.get()
    if not doc.exists:
        raise ValueError(f"Config document not found: {config_document_path}")
    return doc.to_dict()
