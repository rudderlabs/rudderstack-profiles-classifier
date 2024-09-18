def standardize_ref_name(warehouse_type: str, ref_name1: str):
    # This is the equivalent of the StandardizeRefName function in wht.
    # It is not being used by most of pynative models since they use "warehouse.CreateReplaceTableAs"
    # which has this logic built in.
    if warehouse_type == "snowflake":
        return ref_name1.upper()
    if warehouse_type == "redshift":
        return ref_name1.lower()
    return ref_name1
