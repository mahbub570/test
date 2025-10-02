
import json
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.error import HTTPError

try:
    from scribe_data.wikidata.wikidata_utils import sparql
except ImportError:
    # Fallback if dependencies are not available
    print("Warning: SPARQLWrapper not available. Please install with: pip install SPARQLWrapper")
    sparql = None


def load_metadata_files(language_metadata_path: str = None, data_type_metadata_path: str = None) -> tuple:
    """
    Load language and data type metadata from JSON files.
    
    Parameters
    ----------
    language_metadata_path : str, optional
        Path to language_metadata.json file
    data_type_metadata_path : str, optional  
        Path to data_type_metadata.json file
        
    Returns
    -------
    tuple
        (language_metadata, data_type_metadata) dictionaries
    """
    # Default paths relative to this file
    current_dir = Path(__file__).parent.parent.parent
    
    if language_metadata_path is None:
        language_metadata_path = current_dir / "resources" / "language_metadata.json"
    if data_type_metadata_path is None:
        data_type_metadata_path = current_dir / "resources" / "data_type_metadata.json"
    
    with open(language_metadata_path, 'r') as f:
        language_metadata = json.load(f)
    
    with open(data_type_metadata_path, 'r') as f:
        data_type_metadata = json.load(f)
        
    return language_metadata, data_type_metadata


def execute_sparql_with_retry(query: str, max_retries: int = 5, delay: float = 5.0) -> Optional[Dict]:
    """
    Execute SPARQL query with retry logic and timeout handling.
    
    Parameters
    ----------
    query : str
        SPARQL query to execute
    max_retries : int, default 5
        Maximum number of retry attempts
    delay : float, default 5.0
        Base delay between retries in seconds
        
    Returns
    -------
    Optional[Dict]
        Query results or None if failed
    """
    if sparql is None:
        print("Error: SPARQL functionality not available. Please install required dependencies.")
        return None
        
    for attempt in range(max_retries):
        try:
            sparql.setQuery(query)
            results = sparql.query().convert()
            return results
            
        except HTTPError as e:
            if "429" in str(e):  # Too Many Requests
                wait_time = delay * (2 ** attempt)  # Exponential backoff for rate limiting
                print(f"Rate limited (429) on attempt {attempt + 1}. Waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"HTTPError on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(delay * (attempt + 1))
                else:
                    print(f"Max retries ({max_retries}) reached. Query failed.")
                    return None
                    
        except Exception as e:
            error_msg = str(e)
            if "QueryBadFormed" in error_msg or "badly formed" in error_msg:
                print(f"Query syntax error: {e}")
                print("Query that failed:")
                print(query)
                return None  # Don't retry syntax errors
            else:
                print(f"Unexpected error on attempt {attempt + 1}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(delay * (attempt + 1))
                else:
                    print(f"Max retries ({max_retries}) reached. Query failed.")
                    return None
    
    return None


def filtering(lang_qid: str, data_qid: str, use_limit: bool = True, limit_count: int = 1000) -> Optional[List[Dict]]:
    """
    Filter and analyze grammatical feature combinations for lexemes with timeout handling.
    
    Parameters
    ----------
    lang_qid : str
        Language QID (e.g., "Q1860" for English)
    data_qid : str
        Data type QID (e.g., "Q1084" for nouns)
    use_limit : bool, default True
        Whether to limit results to prevent timeouts
    limit_count : int, default 1000
        Maximum number of results to return
        
    Returns
    -------
    Optional[List[Dict]]
        List of form combinations with QIDs or None if failed
    """
    # Simplified and more robust query to prevent syntax errors
    query = f"""PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX ontolex: <http://www.w3.org/ns/lemon/ontolex#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT ?comboQIDs (COUNT(?form) AS ?formsWithThisCombo) WHERE {{
  {{
    SELECT ?form (GROUP_CONCAT(DISTINCT REPLACE(STR(?feature), ".*/(Q[0-9]+)", "$1"); separator="|") AS ?comboQIDs) WHERE {{
      ?lexeme dct:language wd:{lang_qid} ;
              wikibase:lexicalCategory wd:{data_qid} ;
              ontolex:lexicalForm ?form .
      ?form wikibase:grammaticalFeature ?feature .
    }}
    GROUP BY ?form
    {"LIMIT " + str(limit_count) if use_limit else ""}
  }}
  FILTER(STRLEN(?comboQIDs) > 0)
}}
GROUP BY ?comboQIDs
ORDER BY DESC(?formsWithThisCombo)"""
    
    print(f"Executing query for language {lang_qid}, data type {data_qid}")
    results = execute_sparql_with_retry(query)
    
    if results is None:
        return None
        
    # Process results
    bindings = results.get("results", {}).get("bindings", [])
    processed_results = []
    
    for binding in bindings:
        combo_qids = binding.get("comboQIDs", {}).get("value", "")
        form_count = binding.get("formsWithThisCombo", {}).get("value", "0")
        
        if combo_qids:  # Only include results with QIDs
            qid_list = [qid.strip() for qid in combo_qids.split("|") if qid.strip()]
            processed_results.append({
                "qids": qid_list,
                "count": int(form_count)
            })
    
    return processed_results


def process_all_languages_and_types(
    language_metadata_path: str = None, 
    data_type_metadata_path: str = None,
    output_file: str = "filtered_forms_output.json"
) -> Dict:
    """
    Process all languages and data types from metadata files and format output.
    
    Parameters
    ----------
    language_metadata_path : str, optional
        Path to language_metadata.json file
    data_type_metadata_path : str, optional
        Path to data_type_metadata.json file  
    output_file : str, default "filtered_forms_output.json"
        Output file name
        
    Returns
    -------
    Dict
        Formatted results in structure: {lang_qid: {type_qid: [unique_qid_forms]}}
    """
    language_metadata, data_type_metadata = load_metadata_files(
        language_metadata_path, data_type_metadata_path
    )
    
    # Build the output structure
    output_data = {}
    
    # Extract language QIDs
    language_qids = {}
    for lang_name, lang_data in language_metadata.items():
        if "qid" in lang_data:
            language_qids[lang_data["qid"]] = lang_name
        elif "sub_languages" in lang_data:
            for sub_lang_name, sub_lang_data in lang_data["sub_languages"].items():
                if "qid" in sub_lang_data:
                    language_qids[sub_lang_data["qid"]] = f"{lang_name}_{sub_lang_name}"
    
    # Process each language and data type combination
    total_combinations = len(language_qids) * len(data_type_metadata)
    current_combination = 0
    
    for lang_qid, lang_name in language_qids.items():
        print(f"\nProcessing language: {lang_name} ({lang_qid})")
        output_data[lang_qid] = {}
        
        for data_type_name, data_type_qid in data_type_metadata.items():
            if not data_type_qid:  # Skip empty QIDs
                continue
                
            current_combination += 1
            print(f"  Processing {data_type_name} ({data_type_qid}) - {current_combination}/{total_combinations}")
            
            # Get filtered results for this combination
            results = filtering(lang_qid, data_type_qid)
            
            if results is not None:
                # Extract unique QID forms
                unique_qid_forms = set()
                for result in results:
                    for qid in result["qids"]:
                        unique_qid_forms.add(qid)
                
                output_data[lang_qid][data_type_qid] = sorted(list(unique_qid_forms))
                print(f"    Found {len(unique_qid_forms)} unique QID forms")
            else:
                output_data[lang_qid][data_type_qid] = []
                print("    No results (timeout or error)")
            
            # Longer delay to prevent rate limiting (429 errors)
            time.sleep(10)  # 10 second delay between requests
    
    # Save results
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\nResults saved to {output_file}")
    return output_data


def process_limited_subset(
    max_languages: int = 5,
    max_data_types: int = 3,
    language_metadata_path: str = None,
    data_type_metadata_path: str = None,
    output_file: str = "limited_filtered_forms_output.json"
) -> Dict:
    """
    Process a limited subset of languages and data types to avoid rate limiting.
    
    Parameters
    ----------
    max_languages : int, default 5
        Maximum number of languages to process
    max_data_types : int, default 3
        Maximum number of data types to process
    language_metadata_path : str, optional
        Path to language_metadata.json file
    data_type_metadata_path : str, optional
        Path to data_type_metadata.json file  
    output_file : str, default "limited_filtered_forms_output.json"
        Output file name
        
    Returns
    -------
    Dict
        Formatted results in structure: {lang_qid: {type_qid: [unique_qid_forms]}}
    """
    language_metadata, data_type_metadata = load_metadata_files(
        language_metadata_path, data_type_metadata_path
    )
    
    # Build the output structure
    output_data = {}
    
    # Extract language QIDs (limited)
    language_qids = {}
    count = 0
    for lang_name, lang_data in language_metadata.items():
        if count >= max_languages:
            break
        if "qid" in lang_data:
            language_qids[lang_data["qid"]] = lang_name
            count += 1
        elif "sub_languages" in lang_data:
            for sub_lang_name, sub_lang_data in lang_data["sub_languages"].items():
                if count >= max_languages:
                    break
                if "qid" in sub_lang_data:
                    language_qids[sub_lang_data["qid"]] = f"{lang_name}_{sub_lang_name}"
                    count += 1
    
    # Limit data types
    limited_data_types = dict(list(data_type_metadata.items())[:max_data_types])
    
    # Process each language and data type combination
    total_combinations = len(language_qids) * len(limited_data_types)
    current_combination = 0
    
    print(f"Processing {len(language_qids)} languages and {len(limited_data_types)} data types")
    print(f"Total combinations: {total_combinations}")
    
    for lang_qid, lang_name in language_qids.items():
        print(f"\nProcessing language: {lang_name} ({lang_qid})")
        output_data[lang_qid] = {}
        
        for data_type_name, data_type_qid in limited_data_types.items():
            if not data_type_qid:  # Skip empty QIDs
                continue
                
            current_combination += 1
            print(f"  Processing {data_type_name} ({data_type_qid}) - {current_combination}/{total_combinations}")
            
            # Get filtered results for this combination with smaller limit
            results = filtering(lang_qid, data_type_qid, use_limit=True, limit_count=100)
            
            if results is not None:
                # Extract unique QID forms
                unique_qid_forms = set()
                for result in results:
                    for qid in result["qids"]:
                        unique_qid_forms.add(qid)
                
                output_data[lang_qid][data_type_qid] = sorted(list(unique_qid_forms))
                print(f"    Found {len(unique_qid_forms)} unique QID forms")
            else:
                output_data[lang_qid][data_type_qid] = []
                print("    No results (timeout or error)")
            
            # Even longer delay for limited processing to be extra safe
            print("    Waiting 15 seconds before next request...")
            time.sleep(15)  # 15 second delay between requests
    
    # Save results
    with open(output_file, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\nResults saved to {output_file}")
    return output_data


if __name__ == "__main__":
    # Run limited subset by default to avoid overwhelming the server
    print("Running limited subset processing (5 languages, 3 data types)")
    print("This is safer and less likely to hit rate limits.")
    print("=" * 60)
    
    result = process_limited_subset()
    
    print("\n" + "=" * 60)
    print("Limited processing completed!")
    print("To process more data, modify the max_languages and max_data_types parameters")
    print("or use process_all_languages_and_types() for everything (use with caution!)")