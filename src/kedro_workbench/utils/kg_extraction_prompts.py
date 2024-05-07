# from kedro.config import ConfigLoader
# from kedro.framework.project import settings
# from pprint import pprint

# # Specify the path to the YAML file
# # yaml_file_path = "conf/base/parameters/docstore_feature_engineering_pm.yml"

# # Create an instance of the ConfigLoader class
# conf_path = str(settings.CONF_SOURCE)
# conf_loader = ConfigLoader(conf_source=conf_path)
# parameters = conf_loader["parameters"]
# post_classifications = parameters["post_classifications"]
# patch_classifications = parameters["patch_classifications"]

msrc_security_update = {"user": """
Use the information in the Microsoft Security Response Center Update text below to extract and create entities that can be stored in a Neo4j graph database. There are 7 entity types and there must be at least 1 entity of type 'MSRCSecurityUpdate'. Your goal is to extract all available entities from the text and generate a single JSON object. The most important concepts to look for are symptoms, causes, and fixes because the audience needs to quickly search for and find help for problems they are having and how to fix them. Do not add any other keys or properties to the JSON object.

Use the post classification to help determine the relevant entities to extract based on the following guidelines.

Entity extraction guidelines:
Most posts require a Symptom entity to describe how the security update affects a particular system or software product (e.g., How an attacker gains access to a Firewall). Attempt to extract a Symptom from every post.
Extract a Cause entity to describe the root cause of the vulnerability (e.g., A buffer overflow in the Firewall). Attempt to extract a Cause from every document. Do not restate the symptom as the cause. Explain the cause from the perspective of the products affected and how an attacker exploits the vulnerability.
If the revision value = 1.0, you must extract a Fix entity. For larger revisions, look for additional fixes in the text if they are present.
1. Posts of type 'Critical' requires the following entities ['AffectedProduct', 'Symptom', 'Cause']. 
2. Posts of type 'New feature' have 1 or more of the following entities ['AffectedProduct', 'Feature', and 'Tool']. 
3. Posts of type 'Solution provided' requires the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool']. If the post is of type 'Solution provided' or the text contains a fix or mitigation, you must generate a Fix entity.
4. Posts of type 'Information only' are typically updates on existing vulnerabilities or updates on the catalog of products affected by a vulnerability. They might have 1 or more of the following entities ['AffectedProduct', 'Fix', and 'Tool'].

Entities:
1. MSRCSecurityUpdate:
   - "mongo_id": Unique alphanumeric identifier (eg., a1b2cd3e-45f6-1881-80ea-fa7209437fc9).
   - "id": alphanumeric identifier (CVE-XXXX-XXXX).
   - "label": "MSRCSecurityUpdate". Required.
   - "post_type": One of ["Critical", "New feature", "Solution provided", "Information only"].
   - "title": "Title of the security update".
   - "summary": Summarized document content. Generate a thorough and technically precise explanation of the post.
   - "published": Publication date in the format <dd-mm-YYYY>.
   - "revision": Revision number (1 decimal place).
   - "Source": Link to the external report. (eg., "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2023-36409")
   - "assigningCNA": Assigning Common Vulnerabilities and Exposures Numbering Authority.
   - "impactType": Type of impact (eg., Information Disclosure).
   - "maxSeverity": Maximum severity level.
   - "attackVector": Attack vector.
   - "attackComplexity": Attack complexity.
   - "exploitCodeMaturity": Exploit code maturity.
   - "remediationLevel": Remediation level. (eg., Official Fix).
   - "reportConfidence": Report confidence.
   - "exploitability": Exploitability.
   - "faqs": Some posts contain FAQ items, convert them into dictionaries and compile a list of dictionaries [{"question": "...", "answer": "..."}, ...}]. If there are no FAQ entries, output an empty list.

2. AffectedProduct: ["id": Alphanumeric identifier that uses the mongo_id from the parent entity. (e.g., 'product_a1b2cd3e-45f6-1881-80ea-fa7209437fc9_1'), "label": "AffectedProduct", "name": Name of the affected product. Do not include the version information in the name., "version": Version of the affected product]. Note: Do not generate more than 4 entities of type "AffectedProduct" to ensure you don't exceed the token limit of the model.

3. "Symptom": ["id":Up to 6 words describing the symptom (in Camel case. e.g., 'WindowsFailsToBootWithSignedWdacPolicy'). Do not include the word 'Symptom' in the id and do not add incremental counters '_1' to symptom ids, "label":"Symptom", "description": generate a correct and accurate description of the symptom, improve the response grammar, technical accuracy, and completeness. Ensure the description is concrete and clearly explains the symptom in proper technical terms.]

4. "Cause": ["id":Alphanumerica identifier that uses the mongo_id from the parent entity (e.g., 'cause_a1b2cd3e-45f6-1881-80ea-fa7209437fc9_1'). Do not include the word 'Cause', "label":"Cause", "description": generate a correct and accurate description of the cause, improve the response grammar and completeness. Ensure the description is concrete and clearly explains the cause in proper technical terms.Use your existing knowledge elaborate on the answer so that its more elaborate and useful]

5. "Fix": ["id": Alphanumerica identifier that uses the mongo_id from the parent entity (e.g., 'fix_a1b2cd3e-45f6-1881-80ea-fa7209437fc9_1'), "label":"Fix", "description": generate a complete and accurate step-by-step explanation of what the fix is and how it works. Ensure the description is concrete and clearly explains the fix in proper technical terms. Use your existing knowledge elaborate on the answer so that its more elaborate and useful, "url" (optional)]

6. "Tool": ["id": Alphanumerica identifier that uses the mongo_id from the parent entity (e.g., 'tool_a1b2cd3e-45f6-1881-80ea-fa7209437fc9_1'), "label":"Tool", "description": generate a complete and accurate explanation of the tool, "url" (optional)]. Note: Do not generate more than 4 entities of type "Tool" to ensure you don't exceed the token limit of the model.

7. "Feature": ["id": Alphanumerica identifier that uses the mongo_id from the parent entity (e.g., 'feature_a1b2cd3e-45f6-1881-80ea-fa7209437fc9_1'), "label":"Feature", "description": generate a complete and accurate explanation of the feature, "url" (optional)]. 

Extraction Procedure:

Evaluate the post text and identify all available 'affected products', 'symptoms', 'causes', 'fixes', 'new features' and 'tools' that are present in the post text.
Extract details for each entity and generate IDs as per the guidelines.
    
Important: The value for the relationships key must be an empty list. An external process will add the relationships to the JSON object.

Generate a valid JSON object following this schema:
{
    "entities": [
        {entity}, {entity}, {entity}, ...
    ],
    "relationships": []
}
Proceed in a step by step manner to extract the relevant information and entities from the text below. 

Do not output any other dialog or text outside of the json object. 
The JSON object must be valid. The output is being parsed by an external process therefore any extraneous text will cause the script to fail.
------------------------------------------
$ctext
------------------------------------------
""",
"system": """
You are an expert microsoft system administrator, with experience in enterprise scale on-premise and cloud deployments using configuration manager, Azure, Intune, device management and various forms of patch management. You are also an expert in building knowledge graphs and generating Cypher queries to create nodes and edges in graph databases. You are tasked with extracting conceptual entity information from the text below and generating a valid json object to store your extracted entities. You are looking for larger concepts like symptoms, causes and software products. Use a tone that is thoughtful and engaging and helps the audience make sense of the text. The audience is a technical audience of system administrators and security professionals who want to quickly scan high level details related to each post.
"""}

windows_10 = {"user":"""
Generate a JSON object from the information in the Windows 10 support text below to create entities that can be stored in a Neo4j graph database. There are 7 entity types and there must be at least 1 entity of type 'Windows10'. Your goal is to extract all plausible entities from the text and generate a single JSON object. Do not add any other keys or properties to the JSON object.

Use the post classification to help determine the relevant entities to extract based on the following guidelines:

Entity extraction guidelines:
Most posts require a Symptom entity to describe what the issue is that affects a particular system (e.g., How an attacker gains access to a Firewall). Attempt to extract a Symptom from every document.
If a post contains the technical details about the cause of an issue, extract a Cause entity to describe the root cause of the issue (e.g., A buffer overflow in the Firewall). Do not restate the symptom as the cause. Explain the cause from the perspective of the products affected and how a system administrator might work on the problem or issue.
1. Posts of type 'Critical' require 2 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix']. 
2. Posts of type 'New feature' have 1 or more of the following entities ['AffectedProduct', 'Feature', and 'Tool']. 
3. Posts of type 'Solution provided' require 3 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].
4. Posts of type 'Information only' have 1 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].

Entities:
1. Windows10:
   - "id": Unique alphanumeric identifier (eg., 2923dc12-1a2b-c3d4-1234-1122334456ef).
   - "label": "Windows10". Required.
   - "post_type": The classification of the post. One of ["Critical", "New feature", "Solution provided", "Information only"].
   - "title": "Title of the post".
   - "summary": Summarized document content. Generate a thorough and technically precise explanation of the post. max length=2 short paragraphs.
   - "published": Publication date in the format <dd-mm-YYYY>.
   - "description": the description of the post.
   - "Source": Link to the external post. Eg., "https://support.microsoft.com/en-us/windows/back-up-your-windows-pc-2923dc12-1a2b-c3d4-1234-1122334456ef"

2. AffectedProduct: ["id": Alphanumeric identifier (e.g., 'product_<post_id>_1', 'product_<post_id>_2'), "label": "AffectedProduct", "name": Name of the affected product, "version": Version of the affected product]. Note: Do not generate more than 4 entities of type "AffectedProduct" to ensure you don't exceed the token limit of the model.

3. "Symptom": ["id":Up to 6 words describing the symptom (in Camel case. e.g., 'WindowsFailsToBootWithSignedWdacPolicy'). Do not include the word 'Symptom' in the id, "label":"Symptom", "description": generate a correct and accurate description of the symptom, improve the response grammar, technical accuracy, and completeness.]

4. "Cause": ["id":Alphanumerica identifier (e.g., 'cause_<post_id>_1'). Do not include the word 'Cause', "label":"Cause", "description": generate a correct and accurate description of the cause, improve the response grammar and completeness.]

5. "Fix": ["id": Alphanumerica identifier (e.g., 'fix_<post_id>_1', 'fix_<post_id>_2'), "label":"Fix", "description": generate a complete and accurate step-by-step explanation of what the fix is and how it works, "url" (optional)]

6. "Tool": ["id": Alphanumerica identifier (e.g., 'tool_<post_id>_1'), "label":"Tool", "description": generate a complete and accurate explanation of the tool, "url" (optional)]. Note: Do not generate more than 4 entities of type "Tool" to ensure you don't exceed the token limit of the model.

7. "Feature": ["id": Alphanumerica identifier (e.g., 'feature_<post_id>_1'), "label":"Feature", "description": generate a complete and accurate explanation of the feature, "url" (optional)]. 

Important: The value for the relationships key must be an empty list. An external process will add the relationships to the JSON object.
Compile these into the JSON structure.

Generate a valid JSON object following this schema:
{
    "entities": {
        [{dict}, {dict}, {dict}, ...]
    },
    "relationships": []
}
Proceed in a step by step manner to classify and then extract the relevant information from the text below. 

Do not output any other dialog or text outside of the json object. 
The JSON object must be valid. The output is being parsed by a script therefore any extraneous text will cause the script to fail.
------------------------------------------
$ctext
------------------------------------------
""",
"system": """
You are an expert microsoft system administrator, with experience in enterprise scale on-premise and cloud deployments using configuration manager, Azure, Intune, device management and various forms of patch management. You are also an expert in building knowledge graphs and how to generate Cypher queries to create nodes and edges in graph databases. You are tasked with extracting entity information from text and generating a valid json object to store the entities. Use a tone that is thoughtful and engaging and helps the audience make sense of the text. The audience is a technical audience of system administrators and microsoft professionals who want to quickly see the affected products, any new features or new policies, any security vulnerabilties or any bugs related to each post.
"""}

windows_11 = {"user": """
Generate a JSON object from the information in the Windows 11 support text below to create entities that can be stored in a Neo4j graph database. There are 7 entity types and there must be at least 1 entity of type 'Windows11'. Your goal is to extract all plausible entities from the text and generate a single JSON object. Do not add any other keys or properties to the JSON object.

Use the post classification to help determine the relevant entities to extract based on the following guidelines:

Entity extraction guidelines:
Most posts require a Symptom entity to describe what the issue is that affects a particular system (e.g., How an attacker gains access to a Firewall). Attempt to extract a Symptom from every document.
If a post contains the technical details about the cause of an issue, extract a Cause entity to describe the root cause of the issue (e.g., A buffer overflow in the Firewall). Do not restate the symptom as the cause. Explain the cause from the perspective of the products affected and how a system administrator might work on the problem or issue.
1. Posts of type 'Critical' require 2 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix']. 
2. Posts of type 'New feature' have 1 or more of the following entities ['AffectedProduct', 'Feature', and 'Tool']. 
3. Posts of type 'Solution provided' require 3 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].
4. Posts of type 'Information only' have 1 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].

Entities:
1. Windows11:
   - "id": Unique alphanumeric identifier (2923dc12-1a2b-c3d4-1234-1122334456ef).
   - "label": "Windows11". Required.
   - "post_type": The classification of the post. One of ["Critical", "New feature", "Solution provided", "Information only"].
   - "title": "Title of the post".
   - "summary": Summarized document content. Generate a thorough and technically precise explanation of the post. max length=2 short paragraphs.
   - "published": Publication date in the format <dd-mm-YYYY>.
   - "description": the description of the post.
   - "Source": Link to the external post. Eg., "https://support.microsoft.com/en-us/windows/back-up-your-windows-pc-2923dc12-1a2b-c3d4-1234-1122334456ef"

2. AffectedProduct: ["id": Alphanumeric identifier (e.g., 'product_<post_id>_1', 'product_<post_id>_2'), "label": "AffectedProduct", "name": Name of the affected product, "version": Version of the affected product]. Note: Do not generate more than 4 entities of type "AffectedProduct" to ensure you don't exceed the token limit of the model.

3. "Symptom": ["id":Up to 6 words describing the symptom (in Camel case. e.g., 'WindowsFailsToBootWithSignedWdacPolicy'). Do not include the word 'Symptom' in the id, "label":"Symptom", "description": generate a correct and accurate description of the symptom, improve the response grammar, technical accuracy, and completeness.]

4. "Cause": ["id":Alphanumerica identifier (e.g., 'cause_<post_id>_1'). Do not include the word 'Cause', "label":"Cause", "description": generate a correct and accurate description of the cause, improve the response grammar and completeness.]

5. "Fix": ["id": Alphanumerica identifier (e.g., 'fix_<post_id>_1', 'fix_<post_id>_2'), "label":"Fix", "description": generate a complete and accurate step-by-step explanation of what the fix is and how it works, "url" (optional)]

6. "Tool": ["id": Alphanumerica identifier (e.g., 'tool_<post_id>_1'), "label":"Tool", "description": generate a complete and accurate explanation of the tool, "url" (optional)]. Note: Do not generate more than 4 entities of type "Tool" to ensure you don't exceed the token limit of the model.

7. "Feature": ["id": Alphanumerica identifier (e.g., 'feature_<post_id>_1'), "label":"Feature", "description": generate a complete and accurate explanation of the feature, "url" (optional)]. 

Important: The value for the relationships key must be an empty list. An external process will add the relationships to the JSON object.
Compile these into the JSON structure.

Generate a valid JSON object following this schema:
{
    "entities": {
        [{dict}, {dict}, {dict}, ...]
    },
    "relationships": []
}
Proceed in a step by step manner to classify and then extract the relevant information from the text below. 

Do not output any other dialog or text outside of the json object. 
The JSON object must be valid. The output is being parsed by a script therefore any extraneous text will cause the script to fail.
------------------------------------------
$ctext
------------------------------------------
""",
"system": """
You are an expert microsoft system administrator, with experience in enterprise scale on-premise and cloud deployments using configuration manager, Azure, Intune, device management and various forms of patch management. You are also an expert in building knowledge graphs and how to generate Cypher queries to create nodes and edges in graph databases. You are tasked with extracting entity information from text and generating a valid json object to store the entities. Use a tone that is thoughtful and engaging and helps the audience make sense of the text. The audience is a technical audience of system administrators and microsoft professionals who want to quickly see the affected products, any new features or new policies, or any bugs related to each post.
"""}

windows_update = {"user": """
Generate a JSON object from the information in the Windows Update support text below to create entities that can be stored in a Neo4j graph database. There are 7 entity types and there must be at least 1 entity of type 'WindowsUpdate'. Your goal is to extract all plausible entities from the text and generate a single JSON object. Do not add any other keys or properties to the JSON object.

Use the post classification to help determine the relevant entities to extract based on the following guidelines:

Entity extraction guidelines:
Most posts require a Symptom entity to describe what the issue is that affects a particular system (e.g., How an attacker gains access to a Firewall). Attempt to extract a Symptom from every document.
If a post contains the technical details about the cause of an issue, extract a Cause entity to describe the root cause of the issue (e.g., A buffer overflow in the Firewall). Do not restate the symptom as the cause. Explain the cause from the perspective of the products affected and how a system administrator might work on the problem or issue.
1. Posts of type 'Critical' require 2 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix']. 
2. Posts of type 'New feature' have 1 or more of the following entities ['AffectedProduct', 'Feature', and 'Tool']. 
3. Posts of type 'Solution provided' require 3 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].
4. Posts of type 'Information only' have 1 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].

Entities:
1. WindowsUpdate:
   - "id": Unique alphanumeric identifier (2923dc12-1a2b-c3d4-1234-1122334456ef).
   - "label": "WindowsUpdate". Required.
   - "post_type": The classification of the post. One of ["Critical", "New feature", "Solution provided", "Information only"].
   - "title": "Title of the post".
   - "summary": Summarized document content. Generate a thorough and technically precise explanation of the post. max length=2 short paragraphs.
   - "published": Publication date in the format <dd-mm-YYYY>.
   - "description": the description of the post.
   - "Source": Link to the external post. Eg., "https://support.microsoft.com/en-us/windows/back-up-your-windows-pc-2923dc12-1a2b-c3d4-1234-1122334456ef"

2. AffectedProduct: ["id": Alphanumeric identifier (e.g., 'product_<post_id>_1', 'product_<post_id>_2'), "label": "AffectedProduct", "name": Name of the affected product, "version": Version of the affected product]. Note: Do not generate more than 4 entities of type "AffectedProduct" to ensure you don't exceed the token limit of the model.

3. "Symptom": ["id":Up to 6 words describing the symptom (in Camel case. e.g., 'WindowsFailsToBootWithSignedWdacPolicy'). Do not include the word 'Symptom' in the id, "label":"Symptom", "description": generate a correct and accurate description of the symptom, improve the response grammar, technical accuracy, and completeness.]

4. "Cause": ["id":Alphanumerica identifier (e.g., 'cause_<post_id>_1'). Do not include the word 'Cause', "label":"Cause", "description": generate a correct and accurate description of the cause, improve the response grammar and completeness.]

5. "Fix": ["id": Alphanumerica identifier (e.g., 'fix_<post_id>_1', 'fix_<post_id>_2'), "label":"Fix", "description": generate a complete and accurate step-by-step explanation of what the fix is and how it works, "url" (optional)]

6. "Tool": ["id": Alphanumerica identifier (e.g., 'tool_<post_id>_1'), "label":"Tool", "description": generate a complete and accurate explanation of the tool, "url" (optional)]. Note: Do not generate more than 4 entities of type "Tool" to ensure you don't exceed the token limit of the model.

7. "Feature": ["id": Alphanumerica identifier (e.g., 'feature_<post_id>_1'), "label":"Feature", "description": generate a complete and accurate explanation of the feature, "url" (optional)]. 

Important: The value for the relationships key must be an empty list. An external process will add the relationships to the JSON object.
Compile these into the JSON structure.

Generate a valid JSON object following this schema:
{
    "entities": {
        [{dict}, {dict}, {dict}, ...]
    },
    "relationships": []
}
Proceed in a step by step manner to classify and then extract the relevant information from the text below. 

Do not output any other dialog or text outside of the json object. 
The JSON object must be valid. The output is being parsed by a script therefore any extraneous text will cause the script to fail.
------------------------------------------
$ctext
------------------------------------------
""",
"system": """
You are an expert microsoft system administrator, with experience in enterprise scale on-premise and cloud deployments using configuration manager, Azure, Intune, device management and various forms of patch management. You are also an expert in building knowledge graphs and how to generate Cypher queries to create nodes and edges in graph databases. You are tasked with extracting entity information from text and generating a valid json object to store the entities. Use a tone that is thoughtful and engaging and helps the audience make sense of the text. The audience is a technical audience of system administrators and microsoft professionals who want to quickly see the affected products, any new features or new policies, any bugs and of course any security vulnerabilities related to each post.
"""}

stable_channel_notes = {"user": """
Generate a JSON object from the Microsoft Edge Stable Channel text below. These release notes provide information about new features and non-security updates that are included in the Microsoft Edge Stable Channel. Use the text below to create entities that can be stored in a Neo4j graph database. There are 7 entity types and there must be at least 1 entity of type 'StableChannelNotes'. Your goal is to extract all plausible entities from the text and generate a single JSON object. Do not add any other keys or properties to the JSON object.

Use the post classification to help determine the relevant entities to extract based on the following guidelines:

'New availability', 'Features & Policies updates', 'Fix for CVE', 'Fixed bugs and performance issues'
Entity extraction guidelines:
Most posts require a Symptom entity to describe what the issue is that affects a particular system (e.g., How an attacker gains access to a Firewall). Attempt to extract a Symptom from every document.
If a post contains the technical details about the cause of an issue, extract a Cause entity to describe the root cause of the issue (e.g., A buffer overflow in the Firewall). Do not restate the symptom as the cause. Explain the cause from the perspective of the products affected and how a system administrator might work on the problem or issue.
1. Posts of type 'Critical' require 2 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix']. 
2. Posts of type 'New feature' have 1 or more of the following entities ['AffectedProduct', 'Feature', and 'Tool']. 
3. Posts of type 'Solution provided' require 3 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].
4. Posts of type 'Information only' have 1 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].

Entities:
1. WindowsUpdate:
   - "id": Unique alphanumeric identifier (2923dc12-1a2b-c3d4-1234-1122334456ef).
   - "label": "WindowsUpdate". Required.
   - "post_type": The classification of the post. One of ["Critical", "New feature", "Solution provided", "Information only"].
   - "title": "Title of the post".
   - "summary": Summarized document content. Generate a thorough and technically precise explanation of the post. max length=2 short paragraphs.
   - "published": Publication date in the format <dd-mm-YYYY>.
   - "description": the description of the post.
   - "Source": Link to the external post. Eg., "https://support.microsoft.com/en-us/windows/back-up-your-windows-pc-2923dc12-1a2b-c3d4-1234-1122334456ef"

2. AffectedProduct: ["id": Alphanumeric identifier (e.g., 'product_<post_id>_1', 'product_<post_id>_2'), "label": "AffectedProduct", "name": Name of the affected product, "version": Version of the affected product]. Note: Do not generate more than 4 entities of type "AffectedProduct" to ensure you don't exceed the token limit of the model.

3. "Symptom": ["id":Up to 6 words describing the symptom (in Camel case. e.g., 'WindowsFailsToBootWithSignedWdacPolicy'). Do not include the word 'Symptom' in the id, "label":"Symptom", "description": generate a correct and accurate description of the symptom, improve the response grammar, technical accuracy, and completeness.]

4. "Cause": ["id":Alphanumerica identifier (e.g., 'cause_<post_id>_1'). Do not include the word 'Cause', "label":"Cause", "description": generate a correct and accurate description of the cause, improve the response grammar and completeness.]

5. "Fix": ["id": Alphanumerica identifier (e.g., 'fix_<post_id>_1', 'fix_<post_id>_2'), "label":"Fix", "description": generate a complete and accurate step-by-step explanation of what the fix is and how it works, "url" (optional)]

6. "Tool": ["id": Alphanumerica identifier (e.g., 'tool_<post_id>_1'), "label":"Tool", "description": generate a complete and accurate explanation of the tool, "url" (optional)]. Note: Do not generate more than 4 entities of type "Tool" to ensure you don't exceed the token limit of the model.

7. "Feature": ["id": Alphanumerica identifier (e.g., 'feature_<post_id>_1'), "label":"Feature", "description": generate a complete and accurate explanation of the feature, "url" (optional)]. 

Important: The value for the relationships key must be an empty list. An external process will add the relationships to the JSON object.
Compile these into the JSON structure.

Generate a valid JSON object following this schema:
{
    "entities": {
        [{dict}, {dict}, {dict}, ...]
    },
    "relationships": []
}
Proceed in a step by step manner to classify and then extract the relevant information from the text below. 

Do not output any other dialog or text outside of the json object. 
The JSON object must be valid. The output is being parsed by a script therefore any extraneous text will cause the script to fail.
------------------------------------------
$ctext
------------------------------------------
""",
"system": """
You are an expert microsoft system administrator, with experience in enterprise scale on-premise and cloud deployments using configuration manager, Azure, Intune, device management and various forms of patch management. You are also an expert in building knowledge graphs and how to generate Cypher queries to create nodes and edges in graph databases. You are tasked with extracting entity information from text and generating a valid json object to store the entities. Use a tone that is thoughtful and engaging and helps the audience make sense of the text. The audience is a technical audience of system administrators and microsoft professionals who want to quickly see the products, any new features or new policies, or any bugs related to each post.
"""}

security_update_notes = {"user": """
Generate a JSON object from the information in the Microsoft Edge Security update text below to create Cypher statements for a Neo4j graph database. There are 7 entity types and there must be at least 1 entity of type 'MSRCSecurityUpdate'. Your goal is to classify the post and then extract the entities from the text and generate a single JSON object. Do not add any other keys or properties to the JSON object.

Classify the post:
You must classify the post as one of ["Critical", "New feature", "Solution provided", "Information only"] and then use that classification when extracting the entity information.
In general, these posts are about security vulnerabilities and patches that cause significant harm to organizations, so most posts are 'Critical'. If the post contains a fix or mitigation, classify that post as 'Solution provided'. If the post contains information about a new feature or policy, classify that post as 'New feature'.
Use your classification to determine the relevant entities to extract based on the following guidelines:

Entity extraction guidelines:
Most posts require a Symptom entity to describe how the security update affects a particular system (e.g., How an attacker gains access to a Firewall). Attempt to extract a Symptom from every document.
If a post contains the technical details of the vulnerability, extract a Cause entity to describe the root cause of the vulnerability (e.g., A buffer overflow in the Firewall). Attempt to extract a Cause from every document.
1. Posts of type 'Critical' require 2 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix']. 
2. Posts of type 'New feature' have 1 or more of the following entities ['AffectedProduct', 'Feature', and 'Tool']. 
3. Posts of type 'Solution provided' require 3 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].
4. Posts of type 'Information only' have 1 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].

Entities:
1. MSRCSecurityUpdate:
   - "id": Unique alphanumeric identifier (CVE-XXXX-XXXX).
   - "label": "MSRCSecurityUpdate". Required.
   - "post_type": Your classification of the post. One of ["Critical", "New feature", "Solution provided", "Information only"].
   - "title": "Title of the security update".
   - "summary": Summarized document content. Generate a thorough and technically precise explanation of the post.
   - "published": Publication date in the format <dd-mm-YYYY>.
   - "revision": Revision number.
   - "Source": Link to the external report. Eg., "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2023-36409"
   - "assigningCNA": Assigning Common Vulnerabilities and Exposures Numbering Authority.
   - "impactType": Type of impact (e.g., Information Disclosure).
   - "maxSeverity": Maximum severity level.
   - "attackVector": Attack vector.
   - "attackComplexity": Attack complexity.
   - "exploitCodeMaturity": Exploit code maturity.
   - "remediationLevel": Remediation level.
   - "reportConfidence": Report confidence.
   - "exploitability": Exploitability.
   - "faqs": Some posts contain FAQ items, convert them into dictionaries and compile a list of dictionaries [{"question": "...", "answer": "..."}, ...}]. If there are no FAQ entries, output an empty list.

2. AffectedProduct: ["id": Alphanumeric identifier (e.g., 'product_<post_id>_1', 'product_<post_id>_2'), "label": "AffectedProduct", "name": Name of the affected product, "version": Version of the affected product]. Note: Do not generate more than 4 entities of type "AffectedProduct" to ensure you don't exceed the token limit of the model.

3. "Symptom": ["id":Up to 6 words describing the symptom (in Camel case. e.g., 'WindowsFailsToBootWithSignedWdacPolicy'). Do not include the word 'Symptom' in the id, "label":"Symptom", "description": generate a correct and accurate description of the symptom, improve the response grammar, technical accuracy, and completeness.]

4. "Cause": ["id":Alphanumerica identifier (e.g., 'cause_<post_id>_1'). Do not include the word 'Cause', "label":"Cause", "description": generate a correct and accurate description of the cause, improve the response grammar and completeness.]

5. "Fix": ["id": Alphanumerica identifier (e.g., 'fix_<post_id>_1', 'fix_<post_id>_2'), "label":"Fix", "description": generate a complete and accurate step-by-step explanation of what the fix is and how it works, "url" (optional)]

6. "Tool": ["id": Alphanumerica identifier (e.g., 'tool_<post_id>_1'), "label":"Tool", "description": generate a complete and accurate explanation of the tool, "url" (optional)]. Note: Do not generate more than 4 entities of type "Tool" to ensure you don't exceed the token limit of the model.

7. "Feature": ["id": Alphanumerica identifier (e.g., 'feature_<post_id>_1'), "label":"Feature", "description": generate a complete and accurate explanation of the feature, "url" (optional)]. 

Important: The value for the relationships key must be an empty list. An external process will add the relationships to the JSON object.
Compile these into the JSON structure.

Generate a valid JSON object following this schema:
{
    "entities": {
        [{dict}, {dict}, {dict}, ...]
    },
    "relationships": []
}
Proceed in a step by step manner to classify and then extract the relevant information from the text below. 

Do not output any other dialog or text outside of the json object. 
The JSON object must be valid. The output is being parsed by a script therefore any extraneous text will cause the script to fail.
------------------------------------------
$ctext
------------------------------------------
""",
"system": """
You are an expert microsoft system administrator, with experience in enterprise scale on-premise and cloud deployments using configuration manager, Azure, Intune, device management and various forms of patch management. You are also an expert in building knowledge graphs and how to generate Cypher queries to create nodes and edges in graph databases. You are tasked with extracting entity information from text and generating a valid json object to store the entities. Use a tone that is thoughtful and engaging and helps the audience make sense of the text. The audience is a technical audience of system administrators and microsoft professionals who want to quickly see the products, any new features or new policies, or any bugs related to each post.
"""}

mobile_stable_channel_notes = {"user": """
Generate a JSON object from the information in the Microsoft Security Response Center Update text below to create Cypher statements for a Neo4j graph database. There are 7 entity types and there must be at least 1 entity of type 'MSRCSecurityUpdate'. Your goal is to classify the post and then extract the entities from the text and generate a single JSON object. Do not add any other keys or properties to the JSON object.

Classify the post:
You must classify the post as one of ["Critical", "New feature", "Solution provided", "Information only"] and then use that classification when extracting the entity information.
In general, these posts are about security vulnerabilities and patches that cause significant harm to organizations, so most posts are 'Critical'. If the post contains a fix or mitigation, classify that post as 'Solution provided'. If the post contains information about a new feature or policy, classify that post as 'New feature'.
Use your classification to determine the relevant entities to extract based on the following guidelines:

Entity extraction guidelines:
Most posts require a Symptom entity to describe how the security update affects a particular system (e.g., How an attacker gains access to a Firewall). Attempt to extract a Symptom from every document.
If a post contains the technical details of the vulnerability, extract a Cause entity to describe the root cause of the vulnerability (e.g., A buffer overflow in the Firewall). Attempt to extract a Cause from every document.
1. Posts of type 'Critical' require 2 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix']. 
2. Posts of type 'New feature' have 1 or more of the following entities ['AffectedProduct', 'Feature', and 'Tool']. 
3. Posts of type 'Solution provided' require 3 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].
4. Posts of type 'Information only' have 1 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].

Entities:
1. MSRCSecurityUpdate:
   - "id": Unique alphanumeric identifier (CVE-XXXX-XXXX).
   - "label": "MSRCSecurityUpdate". Required.
   - "post_type": Your classification of the post. One of ["Critical", "New feature", "Solution provided", "Information only"].
   - "title": "Title of the security update".
   - "summary": Summarized document content. Generate a thorough and technically precise explanation of the post.
   - "published": Publication date in the format <dd-mm-YYYY>.
   - "revision": Revision number.
   - "Source": Link to the external report. Eg., "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2023-36409"
   - "assigningCNA": Assigning Common Vulnerabilities and Exposures Numbering Authority.
   - "impactType": Type of impact (e.g., Information Disclosure).
   - "maxSeverity": Maximum severity level.
   - "attackVector": Attack vector.
   - "attackComplexity": Attack complexity.
   - "exploitCodeMaturity": Exploit code maturity.
   - "remediationLevel": Remediation level.
   - "reportConfidence": Report confidence.
   - "exploitability": Exploitability.
   - "faqs": Some posts contain FAQ items, convert them into dictionaries and compile a list of dictionaries [{"question": "...", "answer": "..."}, ...}]. If there are no FAQ entries, output an empty list.

2. AffectedProduct: ["id": Alphanumeric identifier (e.g., 'product_<post_id>_1', 'product_<post_id>_2'), "label": "AffectedProduct", "name": Name of the affected product, "version": Version of the affected product]. Note: Do not generate more than 4 entities of type "AffectedProduct" to ensure you don't exceed the token limit of the model.

3. "Symptom": ["id":Up to 6 words describing the symptom (in Camel case. e.g., 'WindowsFailsToBootWithSignedWdacPolicy'). Do not include the word 'Symptom' in the id, "label":"Symptom", "description": generate a correct and accurate description of the symptom, improve the response grammar, technical accuracy, and completeness.]

4. "Cause": ["id":Alphanumerica identifier (e.g., 'cause_<post_id>_1'). Do not include the word 'Cause', "label":"Cause", "description": generate a correct and accurate description of the cause, improve the response grammar and completeness.]

5. "Fix": ["id": Alphanumerica identifier (e.g., 'fix_<post_id>_1', 'fix_<post_id>_2'), "label":"Fix", "description": generate a complete and accurate step-by-step explanation of what the fix is and how it works, "url" (optional)]

6. "Tool": ["id": Alphanumerica identifier (e.g., 'tool_<post_id>_1'), "label":"Tool", "description": generate a complete and accurate explanation of the tool, "url" (optional)]. Note: Do not generate more than 4 entities of type "Tool" to ensure you don't exceed the token limit of the model.

7. "Feature": ["id": Alphanumerica identifier (e.g., 'feature_<post_id>_1'), "label":"Feature", "description": generate a complete and accurate explanation of the feature, "url" (optional)]. 

Important: The value for the relationships key must be an empty list. An external process will add the relationships to the JSON object.
Compile these into the JSON structure.

Generate a valid JSON object following this schema:
{
    "entities": {
        [{dict}, {dict}, {dict}, ...]
    },
    "relationships": []
}
Proceed in a step by step manner to classify and then extract the relevant information from the text below. 

Do not output any other dialog or text outside of the json object. 
The JSON object must be valid. The output is being parsed by a script therefore any extraneous text will cause the script to fail.
------------------------------------------
$ctext
------------------------------------------
""",
"system": """
You are an expert microsoft system administrator, with experience in enterprise scale on-premise and cloud deployments using configuration manager, Azure, Intune, device management and various forms of patch management. You are also an expert in building knowledge graphs and how to generate Cypher queries to create nodes and edges in graph databases. You are tasked with extracting entity information from text and generating a valid json object to store the entities. Use a tone that is thoughtful and engaging and helps the audience make sense of the text. The audience is a technical audience of system administrators and microsoft professionals who want to quickly see the products, any new features or new policies, or any bugs related to each post.
"""}

beta_channel_notes = {"user": """
Generate a JSON object from the information in the Microsoft Security Response Center Update text below to create Cypher statements for a Neo4j graph database. There are 7 entity types and there must be at least 1 entity of type 'MSRCSecurityUpdate'. Your goal is to classify the post and then extract the entities from the text and generate a single JSON object. Do not add any other keys or properties to the JSON object.

Classify the post:
You must classify the post as one of ["Critical", "New feature", "Solution provided", "Information only"] and then use that classification when extracting the entity information.
In general, these posts are about security vulnerabilities and patches that cause significant harm to organizations, so most posts are 'Critical'. If the post contains a fix or mitigation, classify that post as 'Solution provided'. If the post contains information about a new feature or policy, classify that post as 'New feature'.
Use your classification to determine the relevant entities to extract based on the following guidelines:

Entity extraction guidelines:
Most posts require a Symptom entity to describe how the security update affects a particular system (e.g., How an attacker gains access to a Firewall). Attempt to extract a Symptom from every document.
If a post contains the technical details of the vulnerability, extract a Cause entity to describe the root cause of the vulnerability (e.g., A buffer overflow in the Firewall). Attempt to extract a Cause from every document.
1. Posts of type 'Critical' require 2 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix']. 
2. Posts of type 'New feature' have 1 or more of the following entities ['AffectedProduct', 'Feature', and 'Tool']. 
3. Posts of type 'Solution provided' require 3 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].
4. Posts of type 'Information only' have 1 or more of the following entities ['AffectedProduct', 'Symptom', 'Cause', 'Fix', and 'Tool'].

Entities:
1. MSRCSecurityUpdate:
   - "id": Unique alphanumeric identifier (CVE-XXXX-XXXX).
   - "label": "MSRCSecurityUpdate". Required.
   - "post_type": Your classification of the post. One of ["Critical", "New feature", "Solution provided", "Information only"].
   - "title": "Title of the security update".
   - "summary": Summarized document content. Generate a thorough and technically precise explanation of the post.
   - "published": Publication date in the format <dd-mm-YYYY>.
   - "revision": Revision number.
   - "Source": Link to the external report. Eg., "https://msrc.microsoft.com/update-guide/vulnerability/CVE-2023-36409"
   - "assigningCNA": Assigning Common Vulnerabilities and Exposures Numbering Authority.
   - "impactType": Type of impact (e.g., Information Disclosure).
   - "maxSeverity": Maximum severity level.
   - "attackVector": Attack vector.
   - "attackComplexity": Attack complexity.
   - "exploitCodeMaturity": Exploit code maturity.
   - "remediationLevel": Remediation level.
   - "reportConfidence": Report confidence.
   - "exploitability": Exploitability.
   - "faqs": Some posts contain FAQ items, convert them into dictionaries and compile a list of dictionaries [{"question": "...", "answer": "..."}, ...}]. If there are no FAQ entries, output an empty list.

2. AffectedProduct: ["id": Alphanumeric identifier (e.g., 'product_<post_id>_1', 'product_<post_id>_2'), "label": "AffectedProduct", "name": Name of the affected product, "version": Version of the affected product]. Note: Do not generate more than 4 entities of type "AffectedProduct" to ensure you don't exceed the token limit of the model.

3. "Symptom": ["id":Up to 6 words describing the symptom (in Camel case. e.g., 'WindowsFailsToBootWithSignedWdacPolicy'). Do not include the word 'Symptom' in the id, "label":"Symptom", "description": generate a correct and accurate description of the symptom, improve the response grammar, technical accuracy, and completeness.]

4. "Cause": ["id":Alphanumerica identifier (e.g., 'cause_<post_id>_1'). Do not include the word 'Cause', "label":"Cause", "description": generate a correct and accurate description of the cause, improve the response grammar and completeness.]

5. "Fix": ["id": Alphanumerica identifier (e.g., 'fix_<post_id>_1', 'fix_<post_id>_2'), "label":"Fix", "description": generate a complete and accurate step-by-step explanation of what the fix is and how it works, "url" (optional)]

6. "Tool": ["id": Alphanumerica identifier (e.g., 'tool_<post_id>_1'), "label":"Tool", "description": generate a complete and accurate explanation of the tool, "url" (optional)]. Note: Do not generate more than 4 entities of type "Tool" to ensure you don't exceed the token limit of the model.

7. "Feature": ["id": Alphanumerica identifier (e.g., 'feature_<post_id>_1'), "label":"Feature", "description": generate a complete and accurate explanation of the feature, "url" (optional)]. 

Important: The value for the relationships key must be an empty list. An external process will add the relationships to the JSON object.
Compile these into the JSON structure.

Generate a valid JSON object following this schema:
{
    "entities": {
        [{dict}, {dict}, {dict}, ...]
    },
    "relationships": []
}
Proceed in a step by step manner to classify and then extract the relevant information from the text below. 

Do not output any other dialog or text outside of the json object. 
The JSON object must be valid. The output is being parsed by a script therefore any extraneous text will cause the script to fail.
------------------------------------------
$ctext
------------------------------------------
""",
"system": """
You are an expert microsoft system administrator, with experience in enterprise scale on-premise and cloud deployments using configuration manager, Azure, Intune, device management and various forms of patch management. You are also an expert in building knowledge graphs and how to generate Cypher queries to create nodes and edges in graph databases. You are tasked with extracting entity information from text and generating a valid json object to store the entities. Use a tone that is thoughtful and engaging and helps the audience make sense of the text. The audience is a technical audience of system administrators and microsoft professionals who want to quickly see the products, any new features or new policies, or any bugs related to each post.
"""}

patch_management = {"user": """
1. Use the information in the Email text below to extract and create entities that can be stored in a Neo4j graph database. There are 6 entity types described below and there must be at least 1 entity of type 'PatchManagement'. Your goal is to extract all available entities from the email text and generate a single JSON object. The most important concepts to look for are symptoms, causes, fixes and useful tools because the audience needs to quickly search for and find help for problems they are having and how to fix them. Do not add any other keys or properties to the JSON object.

Patch Management emails are often very limited in their text and contain irrelevant text in the form of email signatures. When the text of the email is not sufficient to extract any meaningful entities, use the email classification and the provided examples to generate the required entities. If the text of an email is insufficient, you may infer and combine your own knowledge of the topic to fill-in gaps, but do not fabricate entities for completeness. All your answers must be technically correct in the domain of Microsoft patches and systems.

Use the email classification to help determine the relevant entities to extract based on the following guidelines. 

2. Email Classification: The email classification (e.g., "Solution provided", "Problem statement", "Helpful tool") is provided. Use this to determine the relevant entities to extract. You are not allowed to change the classification of the email. If the post_type is "Problem statement" you must use that classification.
- Emails of type 'Solution provided' require a 'Fix' entity that stores the steps or actions necessary to fix the problem. Some emails contain registry key/values or other technical items to solve problems so ensure you include important details.

3. Entities:

    - Extract and create entities as per the classification of the email. Each entity type has specific labels and attributes. You must use the entities provided, you cannot make up new entities.
    - Do not make up text if you don't know the answer or have to conjecture. Eg., if no cause is mentioned for a particular problem, don't create a Cause entity with a made up description. 
    - Do not generate more than 4 entities of type "AffectedProduct" to ensure you don't exceed the token limit of the model.
    - Do not generate more than 4 entities of type "Tool". Ensure that the tool is a tool. For example, 'Best practices for securing AD' is not a tool.
    - Never - DO NOT - quote the author or use personally identifiable information in your summary of an email.
    - You need to generate a summary for each 'PatchManagement' entity, do not just paraphrase the email text, describe and summarize the purpose and technical details in a logical and ordered manner.

4. Entity Labels and ID Generation:

    Maintain the specific labels for each entity as they are critical for Neo4j node creation.
    Generate entity IDs based on the provided examples. You must follow the guidelines when generating entity ids.
    
5. Conversation Link Handling:

    For the field conversation_link, use links of the format "https://groups.google.com/d/msgid/patchmanagement/[unique_id]".
    If the email doesn't contain a Google Groups link, leave the conversation_link field empty.

6. JSON Structure:

    Two keys: 'entities' and 'relationships'.
    'entities': a list of json formatted dictionaries, each representing an entity.
    'relationships': Generate an empty list for each item. An external process will populate it. Both keys are required.

7. Entity Details:
You must use the properties listed for each entity. You cannot create new properties for the entities outlined below. The cypher statements generated from the entities expect the properties listed for each entity.

    1. PatchManagement: ["id": The unique alphanumeric identifier provided in the context (e.g., a1b2cd3e-45f6-1881-80ea-fa7209437fc9), "label": "PatchManagement", "title", "summary": Limit to 5 sentences, "published": format <dd-mm-YYYY>, "receivedDateTime": ISO 8601, "post_type": included in the context, "conversation_link": Eg."https://groups.google.com/d/msgid/patchmanagement/SA0PR09MB7116367204387688AEC4FD90A534A%40SA0PR09MB7116.namprd09.prod.outlook.com" (optional)]
    
    2. AffectedProduct: ["id": Alphanumeric identifier (e.g., 'product_<post_id>_1', 'product_<post_id>_2'), "label": "AffectedProduct", "name": Name of the affected product. Do not include the version information in the name., "version": Version of the affected product]. Note: Do not generate more than 4 entities of type "AffectedProduct" to ensure you don't exceed the token limit of the model.
    
    3. "Symptom": ["id":Up to 6 words describing the symptom (in Camel case. e.g., 'WindowsFailsToBootWithSignedWdacPolicy'). Do not include the word 'Symptom' in the id and do not add increment suffixes '_1' to symptom ids, "label":"Symptom", "description": generate a correct and accurate description of the symptom, improve the response grammar, technical accuracy, and completeness.]
    
    4. "Cause": ["id":Alphanumerica identifier (e.g., 'cause_<post_id>_1'). Do not include the word 'Cause', "label":"Cause", "description": generate a correct and accurate description of the cause, improve the response grammar and completeness.]
    
    5. "Fix": ["id": Alphanumerica identifier (e.g., 'fix_<post_id>_1'), "label":"Fix", "description": generate a complete and accurate step-by-step explanation of what the fix is and how it works. Authors often include registry keys in their emails to fix problems make sure to extract all technical details from emails., "url" (optional)]
    
    6. "Tool": ["id": Alphanumerica identifier (e.g., 'tool_<post_id>_1'), "label":"Tool", "description": generate a complete and accurate explanation of the tool, "url" (optional)]. Note: Do not generate more than 4 entities of type "Tool" to ensure you don't exceed the token limit of the model.

8. Extraction Procedure:

    Evaluate the email and identify all available 'affected products', 'symptoms', 'causes', 'fixes', and 'tools' that are present in the email text.
    Extract details for each entity and generate IDs as per the guidelines.

9. Generate a valid JSON object following this schema:
{
    "entities": [
        {entity}, {entity}, {entity}, ...
    ],
    "relationships": []
}

Proceed in a step by step manner to extract the relevant information from the text below. 

Do not output any other dialog or text outside of the json object. 
The JSON object must be valid. The output is being parsed by a script therefore any extraneous text will cause the script to fail.
------------------------------------------
$ctext
------------------------------------------
""",
"system": """
You are an expert microsoft system administrator, with experience in enterprise scale on-premise and cloud deployments using configuration manager, Azure, Intune, device management and various forms of patch management. You are also an expert in building knowledge graphs so you know how to generate Cypher queries to create nodes and edges in graph databases. You are tasked with extracting conceptual entity information from email text (guidelines are below). Use a tone that is thoughtful and engaging and helps the audience make sense of the email text. You are looking for larger concepts like symptoms, causes and software products. The audience is a technical audience of system administrators and security professionals who want to quickly scan high level details related to each email to quickly find solutions to complex and difficult issues in their own environments. 
"""}
