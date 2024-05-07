# Mongo Atlas
def print_mongo_result_properties(result, verbose=False):
    """
    Prints the properties of a MongoDB result object.

    :param result: The MongoDB result object.
    :param verbose: If True, includes explanations for each property.
    """
    properties_explanations = {
        "acknowledged": "Indicates if the operation was acknowledged by MongoDB.",
        "matched_count": "Number of documents that matched the query criteria.",
        "modified_count": "Number of documents that were actually modified.",
        "upserted_count": "Number of documents that were upserted.",
        "upserted_id": "ID of the document that was upserted, if any.",
        "write_errors": "List of write errors encountered during the operation.",
        "write_concern_errors": "List of write concern errors encountered."
    }

    for prop, desc in properties_explanations.items():
        if hasattr(result, prop):
            value = getattr(result, prop)
            print(f"{prop}: {value}", end=" ")
            if verbose:
                print(f"- {desc}")
            else:
                print()

def remove_duplicates(db_collection, duplicates):
    """
    Removes duplicate documents from a MongoDB collection.
    Retains the document with the most metadata keys and deletes the rest.

    :param db_collection: The MongoDB collection object.
    :param duplicates: A list of duplicate document groups.
    """
    deletion_summary = {
        'total_deleted': 0,
        'details': []
    }

    for duplicate_group in duplicates:
        # Fetch full documents for each duplicate ID
        docs = [db_collection.find_one({'_id': dup_id}) for dup_id in duplicate_group['duplicates']]

        # Sort documents by the number of keys (descending) and keep the one with most keys
        docs.sort(key=lambda doc: len(doc.keys()), reverse=True)
        docs_to_delete = docs[1:]  # All except the one with most keys

        for doc in docs_to_delete:
            result = db_collection.delete_one({'_id': doc['_id']})
            deletion_summary['total_deleted'] += result.deleted_count
            deletion_summary['details'].append({
                'id': str(doc['_id']),
                'acknowledged': result.acknowledged,
                'deleted_count': result.deleted_count
            })

    return deletion_summary