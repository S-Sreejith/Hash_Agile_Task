import pysolr

# Solr URL
SOLR_URL = 'http://localhost:8983/solr'

# Function to index data into Solr collection, excluding a specific column
def indexData(p_collection_name, p_exclude_column):
    solr = pysolr.Solr(f"{SOLR_URL}/{p_collection_name}")
    
    # Example employee data to index
    employee_data = [
        {"id": "E02001", "Full Name": "Alice", "Department": "IT", "Gender": "Female"},
        {"id": "E02002", "Full Name": "Bob", "Department": "HR", "Gender": "Male"},
        {"id": "E02003", "Full Name": "Charlie", "Department": "Finance", "Gender": "Male"}
    ]

    # Exclude the specified column
    for emp in employee_data:
        if p_exclude_column in emp:
            del emp[p_exclude_column]

    solr.add(employee_data)
    print(f"Data indexed into {p_collection_name}, excluding column {p_exclude_column}")

# Function to search records by column value
def searchByColumn(p_collection_name, p_column_name, p_column_value):
    solr = pysolr.Solr(f"{SOLR_URL}/{p_collection_name}")
    query = f"{p_column_name}:{p_column_value}"
    try:
        results = solr.search(query)
        print(f"Results for {p_column_name}={p_column_value} in {p_collection_name}:")
        for result in results:
            print(result)
    except pysolr.SolrError as e:
        print(f"Search failed: {e}")

# Function to get the count of employees in a collection
def getEmpCount(p_collection_name):
    solr = pysolr.Solr(f"{SOLR_URL}/{p_collection_name}")
    try:
        query = "*:*"
        results = solr.search(query)
        print(f"Employee count in {p_collection_name}: {results.hits}")
    except pysolr.SolrError as e:
        print(f"Error fetching employee count: {e}")

# Function to delete employee by ID
def delEmpById(p_collection_name, p_employee_id):
    solr = pysolr.Solr(f"{SOLR_URL}/{p_collection_name}")
    try:
        solr.delete(id=p_employee_id)
        print(f"Employee with ID {p_employee_id} deleted from {p_collection_name}")
    except pysolr.SolrError as e:
        print(f"Error deleting employee: {e}")

# Function to retrieve department facet
def getDepFacet(p_collection_name):
    solr = pysolr.Solr(f"{SOLR_URL}/{p_collection_name}")
    try:
        results = solr.search("*:*", **{
            'facet': 'true',
            'facet.field': 'Department'
        })

        print(f"Department Facet for {p_collection_name}:")
        for facet in results.facets['facet_fields']['Department']:
            print(facet)
    except pysolr.SolrError as e:
        print(f"Error fetching department facet: {e}")

# Execute functions
v_nameCollection = 'Hash_sreejith'
v_phoneCollection = 'Hash_4940'

getEmpCount(v_nameCollection)
indexData(v_nameCollection, 'Department')
indexData(v_phoneCollection, 'Gender')
delEmpById(v_nameCollection, 'E02003')
getEmpCount(v_nameCollection)
searchByColumn(v_nameCollection, 'Department', 'IT')
searchByColumn(v_nameCollection, 'Gender', 'Male')
searchByColumn(v_phoneCollection, 'Department', 'IT')
getDepFacet(v_nameCollection)
getDepFacet(v_phoneCollection)
