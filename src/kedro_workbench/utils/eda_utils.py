import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any, Tuple, Callable


def get_doc_keywords(nodes_list: List[Dict[str, Any]], doc_id: str) -> List[str]:
    keywords_list = []
    for node in nodes_list:
        # print(node.metadata)
        if node.metadata["id"] == doc_id:
            # print(f"found doc_id")
            # print(f"keywords {node.metadata['keywords']}")
            # keywords_list = node.metadata["keywords"].split(",")
            keywords_list = [
                item.strip() for item in node.metadata["keywords"].split(",")
            ]
    return keywords_list
