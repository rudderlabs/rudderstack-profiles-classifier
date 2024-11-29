from profiles_rudderstack.material import WhtMaterial


def standardize_ref_name(warehouse_type: str, ref_name: str):
    # This is the equivalent of the StandardizeRefName function in wht.
    # It is not being used by most of pynative models since they use "warehouse.CreateReplaceTableAs"
    # which has this logic built in.
    if warehouse_type == "snowflake":
        return ref_name.upper()
    if warehouse_type == "redshift":
        return ref_name.lower()
    return ref_name


def run_query(wh_client, query: str):
    try:
        result = wh_client.query_sql_with_result(query)
    except:
        raise Exception(f"""Unable to run the following query: {query}""")
    if result is None or result.empty:
        return result
    result.columns = result.columns.str.upper()
    return result
