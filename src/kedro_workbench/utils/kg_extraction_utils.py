import json
import re
import hashlib
import tiktoken
from dataclasses import dataclass
from typing import Dict, Iterator, Tuple, Optional
from neo4j import GraphDatabase, exceptions
import logging
from kedro.config import ConfigLoader
from kedro.framework.project import settings

logger = logging.getLogger(__name__)

conf_path = str(settings.CONF_SOURCE)
conf_loader = ConfigLoader(conf_source=conf_path)
credentials = conf_loader["credentials"]
neo4j_user = credentials['neo4j']['local']["username"]
neo4j_password = credentials['neo4j']['local']["password"]
neo4j_url = credentials['neo4j']['local']["url"]

driver = GraphDatabase.driver(neo4j_url, auth=(neo4j_user, neo4j_password))

encoding = tiktoken.get_encoding("cl100k_base")

@dataclass
class IdMappingManager:
    id_mapping: Dict[str, str] = None

    def __post_init__(self):
        if self.id_mapping is None:
            self.id_mapping = {}

    def add_mapping(self, generated_id: str, existing_id: str):
        # print(f"mapping manager add mapping: {generated_id} -> {existing_id}")
        self.id_mapping[generated_id] = existing_id

    def get(self, generated_id: str) -> Optional[str]:
        # print(f"mapping manager get mapping for {generated_id} -> {self.id_mapping.get(generated_id)}")
        return self.id_mapping.get(generated_id)
    
    def get_mappings(self) -> Dict[str, str]:
        return self.id_mapping

    def get_mappings_iterator(self) -> Iterator[Tuple[str, str]]:
        return iter(self.id_mapping.items())

def extract_id_from_prompt(s):
    # print(f"extracting id from prompt: {s}")
    pattern = r'(?<!post_)id: ([\w-]+)\n'
    match = re.search(pattern, s)

    # Check if the pattern is found
    if match:
        found_id = match.group(1)
        # print(f"Match found: {found_id}")
        return found_id
    else:
        # If no match is found, print the string for inspection
        # print(f"No match found in string: {s}")
        return None

def extract_json_from_text(text):
    # Find the first '{' character
    start_index = text.find('{')
    if start_index != -1:
        # Find the last '}' character
        end_index = text.rfind('}')
        if end_index != -1:
            # Extract the JSON substring
            json_string = text[start_index:end_index + 1]
            return json_string
    return None

def generate_faq_id(entity_id, question):
    hash_object = hashlib.sha1(f"{entity_id}_{question}".encode())
    return f"FAQ_{hash_object.hexdigest()}"

def generate_product_id(entity_id, entity_version, parent_id):
    hash_object = hashlib.sha1(f"{entity_id}_{entity_version}_{parent_id}".encode())
    return f"product_{hash_object.hexdigest()}"

def generate_fix_id(entity_id, parent_id):
    hash_object = hashlib.sha1(f"{entity_id}_{parent_id}".encode())
    return f"fix_{hash_object.hexdigest()}"

def generate_cause_id(entity_id, parent_id):
    hash_object = hashlib.sha1(f"{entity_id}_{parent_id}".encode())
    return f"cause_{hash_object.hexdigest()}"

def escape_cypher_string(s):
    return s.replace('"', '\\"')

def clean_symptom_name(symptom_name):
    symptom_name = symptom_name.replace("symptom", "").replace("Symptom", "").replace("symptom_", "").replace("Symptom_", "")
    return symptom_name

def build_relationships_from_entities(extracted_entities):
    # generate the relationships amongst the entities and store each relationship as a string in the list extracted_entities["relationships"]
    # check if there is a key "entities" in extracted_entities, if not return None
    if "entities" not in extracted_entities:
        return None
    parent_id = None
    for entity in extracted_entities["entities"]:
        if entity["label"] == "PatchManagement":
            parent_id = entity["id"]
            break
        if entity["label"] == "MSRCSecurityUpdate":
            parent_id = entity["mongo_id"]
            break
    # print(f"working with entity: {parent_id}")
    if parent_id is None:
        return extracted_entities

    for i, entity in enumerate(extracted_entities["entities"]):
        relationship_type = None
        if entity["label"] not in ["PatchManagement", "MSRCSecurityUpdate"]:
            relationship_type = None
        if entity["label"] == "AffectedProduct":
            relationship_type = "AFFECTS_PRODUCT"
        elif entity["label"] == "Symptom":
            relationship_type = "HAS_SYMPTOM"
        elif entity["label"] == "Cause":
            relationship_type = "HAS_CAUSE"
        elif entity["label"] == "Fix":
            relationship_type = "HAS_FIX"
        elif entity["label"] == "Tool":
            relationship_type = "HAS_TOOL"
        elif entity["label"] == "Feature":
            relationship_type = "HAS_FEATURE"

        if relationship_type is not None:
            # print(f"{i} found relationship type: {relationship_type}")
            relationship = f"{parent_id}|{relationship_type}|{entity['id']}"
            extracted_entities["relationships"].append(relationship)
           
    return extracted_entities


def neo_query_debug(result_obj):
    debug_info = []

    # Check if result object is not None
    if result_obj:
        # Adding the keys
        keys = result_obj.keys()
        formatted_keys = ", ".join(keys)
        debug_info.append(f"Keys:\n{formatted_keys}\n")

        # Adding the records
        records = result_obj.data()
        formatted_records = "\n".join([str(record) for record in records])
        debug_info.append(f"Records:\n{formatted_records}\n")

        # Adding the summary
        summary = result_obj.consume()
        debug_info.append(f"Summary:\nServer: {summary.server}\nDatabase: {summary.database}\nQuery: {summary.query}\nParameters: {summary.parameters}\n")
    else:
        debug_info.append("No result object available.")

    # Combine all parts into a single string
    debug_output = "\n".join(debug_info)
    return debug_output

def escape_for_neo4j(value):
    # Replace problematic characters
    value = value.replace('\\', '\\\\')
    value = value.replace('"', '\\"')
    value = value.replace('\n', '\\n').replace('\r', '\\r')
    value = value.replace('\t', '\\t')
    # Handle special quotation marks if needed
    value = value.replace('“', "'").replace('”', "'")
    return value

def normalize_title(title):
    # Remove common reply prefixes using a case-insensitive regular expression
    cleaned_title = re.sub(r'(?i)^(re:|fw:)\s*', '', title).strip()
    # Escape the title for safe use in Neo4j queries
    #safe_title = escape_for_neo4j(cleaned_title)
    # print(f"Cleaned title: {cleaned_title}")
    return cleaned_title

def clean_symptom_name(symptom_name):
    symptom_name = symptom_name.replace("symptom", "").replace("Symptom", "").replace("symptom_", "").replace("Symptom_", "")
    return symptom_name

# when id is the primary key of an entity
def extract_id_value(cypher_statement):
    pattern = r'{id: "([^"]+)"\}'
    match = re.search(pattern, cypher_statement)
    if match:
        return match.group(1)
    else:
        return None
# when id is a property of an entity
def extract_id_property(cypher_statement):
    pattern = r'n\.id\s*=\s*"([^"]+)"'
    match = re.search(pattern, cypher_statement)
    if match:
        return match.group(1)
    else:
        return None

def extract_label_from_cypher(cypher_statement):
    pattern = r'MERGE\s*\(\w+:(\w+)\s*{'
    match = re.search(pattern, cypher_statement)
    if match:
        return match.group(1)
    else:
        return None

def extract_title_value(cypher_statement):
    pattern = r'n\.title = "([^"]+)"'
    match = re.search(pattern, cypher_statement)
    if match:
        return match.group(1)
    else:
        return None

def extract_summary_value(cypher_statement):
    pattern = r'n\.summary = "([^"]+)"'
    match = re.search(pattern, cypher_statement)
    if match:
        return match.group(1)
    else:
        return None

def extract_product_name(cypher_statement):
    pattern = r'n\.name = "([^"]+)"'
    match = re.search(pattern, cypher_statement)
    if match:
        return match.group(1)
    else:
        return None

def extract_product_version(cypher_statement):
    pattern = r'n\.version = "([^"]+)"'
    match = re.search(pattern, cypher_statement)
    
    if match:
        return match.group(1)

    return None

def extract_mongo_id(cypher_statement):
    # Regular expression pattern to find the mongo_id value in the Cypher statement
    pattern = r'{mongo_id: "([^"]+)"}'
    match = re.search(pattern, cypher_statement)

    if match:
        return match.group(1)

    return None

# Function to find an existing node
def find_existing_product(product_name, product_version):
    query = """
    MATCH (p:AffectedProduct)
    WHERE toLower(p.name) CONTAINS toLower($product_name)
    AND toLower(p.version) = toLower($product_version)
    RETURN p.id AS id, p.name AS name, p.version AS version
    LIMIT 1
    """
    print(f"Searching for Product: Name - {product_name}, Version - {product_version}")  # Debug print
    with driver.session() as session:
        result = session.run(query, {'product_name': product_name, 'product_version': product_version})
        found_product = result.single()
        if found_product:
            print(f"Found Product ID: {found_product['id']}")  # Debug print
            return found_product
        else:
            # print(f"Product not found: Name - {product_name}, Version - {product_version}")  # Debug print
            # debug_output = neo_query_debug(result)
            # print(debug_output)
            return None
        
def find_existing_symptom(symptom_id):
    query = """
    MATCH (s:Symptom)
    WHERE s.id = $symptom_id
    RETURN s.id AS id, s.description AS description
    LIMIT 1
    """
    # print(f"Searching for Symptom with ID - {symptom_id}")  # Debug print
    with driver.session() as session:
        result = session.run(query, {'symptom_id': symptom_id})
        found_symptom = result.single()
        if found_symptom:
            # print(f"Found Symptom ID: {found_symptom['id']}")  # Debug print
            return found_symptom
        else:
            debug_output = neo_query_debug(result)
            # print(debug_output)
            return None
        
def find_existing_feature(feature_id):
    query = """
    MATCH (s:Feature)
    WHERE s.id = $feature_id
    RETURN s.id AS id, s.description AS description
    LIMIT 1
    """
    # print(f"Searching for Feature with ID - {feature_id}")  # Debug print
    with driver.session() as session:
        result = session.run(query, {'feature_id': feature_id})
        found_feature = result.single()
        if found_feature:
            print(f"Found Feature ID: {found_feature['id']}")  # Debug print
            return found_feature
        else:
            debug_output = neo_query_debug(result)
            # print(debug_output)
            return None

def find_existing_patch_management(title):
    query = """
    MATCH (pm:PatchManagement)
    WHERE toLower(pm.title) = toLower($title)
    RETURN pm.id AS id, 
       pm.title AS title, 
       pm.summary AS summary, 
       pm.published AS published, 
       pm.receivedDateTime AS receivedDateTime, 
       pm.post_type AS post_type, 
       pm.conversation_link AS conversation_link
    LIMIT 1

    """
    # print(f"Searching for PatchManagement with Title - {title}")  # Debug print
    with driver.session() as session:
        result = session.run(query, {'title': title})
        found_patch_management = result.single()
        if found_patch_management:
            # print(f"Found PatchManagement ID: {found_patch_management['id']}")  # Debug print
            return found_patch_management
        else:
            debug_output = neo_query_debug(result)
            # print(debug_output)
            return None

def find_existing_msrc_security_update(mongo_id):
    print("searching for existing msrc post for patch management relationship")
    properties = [
        'id', 'post_type', 'title', 'summary', 'published',
        'revision', 'Source', 'assigningCNA', 'impactType', 'maxSeverity',
        'attackVector', 'attackComplexity', 'exploitCodeMaturity', 'remediationLevel',
        'reportConfidence', 'exploitability'
    ]

    # Constructing the properties part of the RETURN statement
    return_properties = ', '.join([f'n.{prop} AS {prop}' for prop in properties])

    query = f"""
    MATCH (n:MSRCSecurityUpdate)
    WHERE n.mongo_id = $mongo_id
    RETURN n.mongo_id AS mongo_id, {return_properties}
    LIMIT 1
    """
    # print(f"Searching for MSRCSecurityUpdate with mongo_id - {mongo_id}")  # Debug print
    with driver.session() as session:
        result = session.run(query, {'mongo_id': mongo_id})
        found_update = result.single()
        if found_update:
            print(f"Found MSRCSecurityUpdate with mongo_id: {found_update['mongo_id']}")  # Debug print
            return found_update
        else:
            print("no existing msrc found")
            return None

def standardize_version_value(statement):
    # Updated regex pattern to correctly capture the required groups
    pattern = r'(n\.version\s*=\s*")([^"]*)"'

    values_to_replace = ["Not specified", "Unknown", "", "-", "Not provided"]

    def replace_version(match):
        version_key = match.group(1)  # Capturing 'n.version = "'
        version_value = match.group(2)  # Extracting the version value

        # Check if the version value is in the list of values to be replaced
        if version_value.lower() in [val.lower() for val in values_to_replace]:
            return version_key + 'None"'  # Replacing with 'None'

        return match.group(0)  # Return the original match if no replacement is necessary

    return re.sub(pattern, replace_version, statement, flags=re.IGNORECASE)


def find_cve_ids(text):
    pattern = r'CVE-\d{4,5}-\d{4,5}'
    matches = re.finditer(pattern, text, re.IGNORECASE)
    cve_ids = [match.group() for match in matches]
    
    return cve_ids

def replace_ids_in_statement(statement: str, id_mapping_manager: IdMappingManager) -> str:
    pattern = r'\{id: "([^"]+)"\}'

    def replace_match(match):
        generated_id = match.group(1)
        existing_id = id_mapping_manager.get(generated_id)
        if existing_id:
            return f'{{id: "{existing_id}"}}'
        else:
            return match.group(0)  # Return the original match if no mapping is found

    return re.sub(pattern, replace_match, statement)

def find_and_link_patch_management(title):
    # Assuming title normalization is required
    normalized_title = normalize_title(title)
    # print(f"looking for patchmanagement with title: {normalized_title}")
    query = """
    MATCH (pm:PatchManagement)
    WHERE toLower(pm.title) CONTAINS toLower($normalized_title)
    WITH pm
    ORDER BY pm.receivedDateTime
    WITH collect(pm) AS pms
    UNWIND range(0, size(pms)-2) AS idx
    WITH pms[idx] AS pm1, pms[idx+1] AS pm2
    WHERE NOT (pm1)-[:FOLLOWED_BY]->(pm2)
    MERGE (pm1)-[:FOLLOWED_BY]->(pm2)
    RETURN pm1.id AS id1, pm2.id AS id2
    """
    relationship_statements = []
    with driver.session() as session:
        result = session.run(query, {'normalized_title': normalized_title})
        
        for idx, record in enumerate(result, start=1):
            # print(f"found topic {idx} -> {record}")
            link_query = f"""MERGE (a:PatchManagement {{id: '{record['id1']}'}}) MERGE (b:PatchManagement {{id: '{record['id2']}'}}) MERGE (a)-[:FOLLOWED_BY]->(b)"""
            relationship_statements.append(link_query.strip())
            # print(f"attempted to connect two patch management entities: {link_query}")
        
        if not relationship_statements:
            print("No patch emails shared that topic.\n")

    return relationship_statements

def find_and_link_security_updates(patch_management_id, title, summary):
    # Extract CVE IDs from title and summary
    # print(f"patch title and summary to find related CVEs: {title}\n{summary}\n")
    cve_ids_in_title = find_cve_ids(title)
    cve_ids_in_summary = find_cve_ids(summary)

    relationship_statements = []

    with driver.session() as session:
        # Loop through each extracted CVE ID and create Cypher statements
        for cve_id in set(cve_ids_in_title + cve_ids_in_summary):
            # this query is executed, multiline is fine
            find_query = f"""
            MATCH (su:MSRCSecurityUpdate) 
            WHERE su.id = '{cve_id}' 
            RETURN su.mongo_id AS mongo_id
            """
            security_updates = session.run(find_query).value()
            # print(f"find CVEs returns: {security_updates}\n{find_query}\n")
            for su_mongo_id in security_updates:
                # this query is written to file so don't use multiline and strip out all whitespace
                link_query = f"""MERGE (pm:PatchManagement {{id: '{patch_management_id}'}}) MERGE (su:MSRCSecurityUpdate {{mongo_id: '{su_mongo_id}'}}) MERGE (pm)-[:REFERENCES]->(su)"""
                # Write the query to the file without any leading/trailing white spaces
                relationship_statements.append(link_query)

    # return Cypher statements
    return relationship_statements

def link_to_previous_revision(cve_id, current_revision, current_mongo_id):
    current_revision_number = convert_revision_to_float(current_revision)
    # print(f"current revision: {current_revision_number}")
    # print(f"current post_id: {cve_id}")
    
    previous_revision_mongo_id = None
    if current_revision_number > 10:
        with driver.session() as session:
            # Retrieve all entities with the same CVE ID
            find_query = f"""
            MATCH (su:MSRCSecurityUpdate)
            WHERE su.id = '{cve_id}'
            RETURN su.mongo_id AS mongo_id, su.revision AS revision
            """
            entities = session.run(find_query).data()

            # Print the find query and result for debugging
            # print("Find Query:", find_query)
            # print("Entities Found:", entities)

            # Convert revisions to floats and sort
            sorted_entities = sorted(entities, key=lambda x: convert_revision_to_float(x['revision']))

            # Find the next largest revision
            for entity in sorted_entities:
                # print(f"finding previous revisions. entity found -> {entity}")
                entity_revision_number = convert_revision_to_float(entity['revision'])
                if entity_revision_number < current_revision_number:
                    previous_revision_mongo_id = entity['mongo_id']
                    print(f"what is the previous entity mongo_id? {entity['mongo_id']}")
                else:
                    print(f"found previous revision {entity_revision_number} not smaller than current revision {current_revision_number}")
                    break
            # Print the previous revision mongo_id for debugging
            # print("Previous Revision mongo_id:", previous_revision_mongo_id)

    # If a previous revision is found, create the link
    relationship_statement = []
    if previous_revision_mongo_id:
        link_query = f"""MATCH (current:MSRCSecurityUpdate {{mongo_id: '{current_mongo_id}'}}), (previous:MSRCSecurityUpdate {{mongo_id: '{previous_revision_mongo_id}'}}) MERGE (current)-[:PREVIOUS_REVISION]->(previous)"""
        relationship_statement.append(link_query.strip())

        # Print the link query for debugging
        # print("Link Query:", link_query)

    return relationship_statement


def convert_revision_to_float(revision):
    try:
        # Convert revision string to float for sorting
        # remove decimal point. 1.0 -> 10, 1.1 -> 11.
        return float(revision.replace('.', ''))
    except ValueError:
        return 0.0
    
def product_record_to_cypher(record):
    # Extracting properties from the record and escaping quotes
    # print("generating cypher for existing product")
    id_value = record.get('id', '')
    name_value = record.get('name', '')
    version_value = standardize_version_value(record.get('version', ''))
    version_value = version_value
    #print(f"product_record_to_cypher id: {id_value}\nname: {name_value}\nversion: {version_value}\n")
    # Constructing the Cypher MERGE statement
    cypher_statement = f"""MERGE (n:AffectedProduct {{id: "{id_value}"}}) ON CREATE SET n.name = "{name_value}", n.version = "{version_value}" """

    return cypher_statement


def symptom_record_to_cypher(record):
    # Extracting properties from the record and escaping quotes
    # print("generating cypher for existing symptom")
    id_value = record.get('id', '')
    description_value = record.get('description', '')

    # Constructing the Cypher MERGE statement
    cypher_statement = f"""MERGE (n:Symptom {{id: "{id_value}"}}) ON CREATE SET n.description = "{description_value}" """
    return cypher_statement

def feature_record_to_cypher(record):
    # Extracting properties from the record and escaping quotes
    # print("generating cypher for existing feature")
    id_value = record.get('id', '')
    description_value = record.get('description', '')
    url_value = record.get('url', '')
    
    cypher_statement = f"""MERGE (n:Feature {{id: "{id_value}"}}) ON CREATE SET n.description = "{description_value}", n.url = "{url_value}" """
    return cypher_statement

def msrc_security_update_record_to_cypher(record):
    print("DEPRACTED FUNCTION SHOULDNT BE CALLED")
    # Extracting properties from the record
    # print("generating cypher for existing msrc")
    properties = [
        'mongo_id', 'post_type', 'title', 'summary', 'published',
        'revision', 'Source', 'assigningCNA', 'impactType', 'maxSeverity',
        'attackVector', 'attackComplexity', 'exploitCodeMaturity', 'remediationLevel',
        'reportConfidence', 'exploitability'
    ]
    
    # Function to escape quotes and format each property assignment
    def format_assignment(prop):
        value = record.get(prop)
        if value is None:
            value = ''
        else:
            value = value.replace('"', '\\"')
        return f'n.{prop} = "{value}"'

    # Constructing the property part of the Cypher statement
    property_assignments = ', '.join([format_assignment(prop) for prop in properties])
    
    # ID value with escaped quotes
    id_value = record.get('id', '').replace('"', '\\"')

    # Constructing the complete Cypher MERGE statement
    cypher_statement = f'MERGE (n:MSRCSecurityUpdate {{id: "{id_value}"}}) SET {property_assignments}'
    print(f"existing msrc post found, generating cypher:\n{cypher_statement}")
    return cypher_statement

def is_relationship_statement(statement):
    # Simple heuristic: relationship statements likely have more than one MERGE
    num_merge = statement.count("MERGE")
    return num_merge > 1

def extract_second_id(statement):
    # Regular expression pattern to find the second ID in the statement
    pattern = r'MERGE \([a-z]:[A-Za-z]+ \{(?:id|mongo_id): "([^"]+)"\}\) MERGE \([a-z]:([A-Za-z]+) \{(?:id|mongo_id): "([^"]+)"\}\)'
    match = re.search(pattern, statement)
    if match:
        node_type = match.group(2)
        second_id = match.group(3)
        return node_type, second_id
    return None, None

def extract_revision_from_cypher(cypher_statement):
    # Regular expression pattern to find the revision value
    pattern = r"n\.revision\s*=\s*\"([^\"]+)\""
    match = re.search(pattern, cypher_statement)
    if match:
        return match.group(1)  # Return the captured revision value
    else:
        return None

def process_relationship_statement(statement, id_mapping_manager):
    print("checking if relationship statement needs to be updated")
    print(f"relationship statement: {statement}")
    node_type, second_id = extract_second_id(statement)
    print(f"node_type: {node_type}\nsecond_id: {second_id}")

    # Check if node_type and second_id are extracted correctly
    if node_type is None or second_id is None:
        print(f"Failed to extract node_type or second_id from statement: {statement}")
        return statement

    id_mapping = id_mapping_manager.get_mappings()
    if node_type in ["AffectedProduct", "Symptom", "Feature"] and second_id in id_mapping:
        print(f"id_mapping dict at second id: {id_mapping[second_id]}")
        new_id = id_mapping[second_id]
        pattern = f'({node_type} \\{{id: "{second_id}"\\}})'
        updated_statement = re.sub(pattern, f'{node_type} {{id: "{new_id}"}}', statement)
        print(f"statement updated {updated_statement}")
        return updated_statement
    return statement

def execute_cypher(stmt):
    try:
        # print(f"executing cypher statement: {stmt}")
        with driver.session() as session:
            result = session.run(stmt)
            # print(result)
            # You can check if the result object has any summary or other indicators of success.
            # For many write operations, just not having an exception is a good indicator of success.
            return True  # Indicates successful execution
    except exceptions.Neo4jError as e:
        print(f"An error occurred executing neo4j statement: {e}")
        with open("data/03_primary/failed_cypher_statements.txt", "a") as f:
            f.write(f"{stmt} - Exception: {e}\n")
        return False
    
def augment_cypher_statements(cypher_statement: str, id_mapping_manager: IdMappingManager):
    multi_step_entity_types = [
        ("PatchManagement", "patch_management"), 
        ("MSRCSecurityUpdate", "msrc_security_update"),
        ("Symptom", "symptom"), 
        ("AffectedProduct", "product"), 
        ("Feature", "feature")
    ]
    single_step_entity_types = [
        ("Cause", "cause"), 
        ("Fix", "fix"), 
        ("FAQ", "faq"), 
        ("Tool", "tool")
    ]
    augmented_entities = []
    augmented_relationships = []
    # print(f"processing cypher statement: {cypher_statement}\n")
    # store statements in a list
    
    if not is_relationship_statement(cypher_statement):
        # print("processing an entity statement\n")
        for entity_type, entity_type_lower in multi_step_entity_types:
            generated_id = None
            title = None
            summary = None
            existing_entity = None
            
            if f"MERGE (n:{entity_type}" in cypher_statement:
                
                if entity_type_lower == "symptom":
                    # print(f"processing a symptom\n")
                    generated_id = extract_id_value(cypher_statement)
                    # existing_entity equals an id or None
                    existing_entity = globals()[f"find_existing_{entity_type_lower}"](generated_id)
                    # print(f"existing symptom: {existing_entity}\n")
                    
                elif entity_type_lower == "product":
                    # print(f"processing a product\n")
                    pattern = r'(n\.version\s*=\s*")[^"]*(")'
                    generated_id = extract_id_value(cypher_statement)
                    product_name = extract_product_name(cypher_statement)
                    standardized_cypher = standardize_version_value(cypher_statement)
                    product_version = extract_product_version(standardized_cypher)
                    cypher_statement = standardized_cypher
                    # print(f"product data before lookup: {product_name}, {product_version}\n")
                    # existing_entity equals an id or None
                    existing_entity = globals()[f"find_existing_{entity_type_lower}"](product_name, product_version)
                    # print(f"existing product found: {existing_entity}\n")
                    
                elif entity_type_lower == "feature":
                    # print(f"processing a feature\n")
                    generated_id = extract_id_value(cypher_statement)
                    # existing_entity equals an id or None
                    existing_entity = globals()[f"find_existing_{entity_type_lower}"](generated_id)
                    
                elif entity_type_lower == "patch_management":
                    # print("processing patch management\n")
                    generated_id = extract_id_value(cypher_statement)
                    title = extract_title_value(cypher_statement)
                    summary = extract_summary_value(cypher_statement)
                    # print("looking for existing msrc posts that might relate.")
                    # store found msrc relationship statements in a list
                    new_msrc_relationships = find_and_link_security_updates(generated_id, title, summary)
                    # print(f"found {len(new_msrc_relationships)} new msrc relationships\n")
                    if new_msrc_relationships:
                        augmented_relationships.extend(new_msrc_relationships)
                    # for new_relation in new_msrc_relationships:
                    #     print("adding discovered relationship")
                        # file.write(f"{new_relation}\n")
                        # success = execute_cypher(new_relation)
                        # if success:
                        #     print("inserted discovered msrc relationship\n")
                        # else:
                        #     print("error inserting discovered msrc relationship\n")
                    
                    related_topic_relationships = find_and_link_patch_management(title)
                    if related_topic_relationships:
                        augmented_relationships.extend(related_topic_relationships)
                    # print(f"found {len(related_topic_relationships)} topic thread relationships\n")
                    # for new_relation in related_topic_relationships:
                    #     print("adding discovered topic relationship")
                        # file.write(f"{new_relation}\n")
                        # success = execute_cypher(new_relation)
                        # if success:
                        #     print("inserted discovered topic relationship\n")
                        # else:
                        #     print("error inserting discovered topic  relationship\n")
                elif entity_type_lower == "msrc_security_update":
                    # print(f"augmenting msrc cypher\n{cypher_statement}\n")
                    post_id = extract_id_property(cypher_statement)   
                    revision = extract_revision_from_cypher(cypher_statement)
                    mongo_id = extract_mongo_id(cypher_statement)
                    # print(f"msrc mongo_id: {mongo_id}\n")
                    new_relationship = link_to_previous_revision(post_id, revision, mongo_id)
                    if new_relationship:
                        print(f"previous revision relationship added")
                        augmented_relationships.extend(new_relationship)
                    
                if existing_entity:
                    existing_id = None
                    if entity_type_lower in ["feature", "symptom", "product"]:
                        # print("found existing entity from valid_types.\n")
                        cypher_statement_existing = globals()[f"{entity_type_lower}_record_to_cypher"](existing_entity)
                        print(f"existing entity found {entity_type_lower} \nreplacement cypher: {cypher_statement_existing}\n")
                        existing_id = extract_id_value(cypher_statement_existing)
                        
                        id_mapping_manager.add_mapping(generated_id, existing_id)
                        # print(f"replaced {entity_type_lower} statement")
                        # augmented_entities.append(cypher_statement_existing)
                        # file.write(f"{cypher_statement_existing}\n")
                        # return to main loop
                        # print(f"num entities: {len(augmented_entities)} num relationships: {len(augmented_relationships)}\n")
                        # print(f"(1) entities: {augmented_entities} relationships: {augmented_relationships}\n")
                        return augmented_entities + augmented_relationships
                
                else:
                    # print(f"No existing {entity_type} found, inserting new {entity_type}")
                    augmented_entities.append(cypher_statement)
                    # file.write(f"{cypher_statement}\n")
                    # success = execute_cypher(cypher_statement)
                    # if success:
                    #     print(f"inserted new {entity_type}\n")
                    # else:
                    #     print(f"error inserting new {entity_type}\n")
                    # # return to main loop
                    # print(f"num entities: {len(augmented_entities)} num relationships: {len(augmented_relationships)}\n") 
                    # print(f"(2) entities: {augmented_entities} relationships: {augmented_relationships}\n")
                    return augmented_entities + augmented_relationships

        # For other entity types, perform standard handling
        for entity_type, entity_type_lower in single_step_entity_types:
            # print(f"processing single step entity {entity_type}\n")
            if f"MERGE (n:{entity_type}" in cypher_statement:
                # print(f"processing single step entity {entity_type}\n")
                augmented_entities.append(cypher_statement)
                # file.write(f"{cypher_statement}\n")
                # success = execute_cypher(cypher_statement)
                # if success:
                #     print(f"inserted new {entity_type}\n")
                # else:
                #     print(f"error inserting new {entity_type}\n")
                # print(f"num entities: {len(augmented_entities)} num relationships: {len(augmented_relationships)}\n") 
                # print(f"(3) entities: {augmented_entities} relationships: {augmented_relationships}\n")
        
        return augmented_entities + augmented_relationships

    else:
        # a relationship statement
        # check if any of the entities initially written to cypher statements contain an id that has been replaced by an existing id
        # print(f"processing a relationship statement")
        corrected_relationship_cypher = process_relationship_statement(cypher_statement, id_mapping_manager)
        # print(f"write_to_file returns -> a {type(corrected_relationship_cypher)} {corrected_relationship_cypher}\n")
        augmented_relationships.append(corrected_relationship_cypher)
        # file.write(f"{corrected_relationship_cypher}\n")
        # success = execute_cypher(corrected_relationship_cypher)
        # if success:
        #     print("inserted corrected relationship\n")
        # else:
        #     print("error inserting corrected relationship\n")
        # print(f"num entities: {len(augmented_entities)} num relationships: {len(augmented_relationships)}\n") 
        # print(f"(4) entities: {augmented_entities} relationships: {augmented_relationships}\n")
        return augmented_entities + augmented_relationships
