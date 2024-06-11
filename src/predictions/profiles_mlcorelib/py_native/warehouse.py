def standardize_ref_name(warehouse_type: str, ref_name: str):
    # This is the equivalent of the StandardizeRefName function in wht.
    # It is not being used by most of pynative models since they use "warehouse.CreateReplaceTableAs"
    # which has this logic built in.
    if warehouse_type == "snowflake":
        return ref_name.upper()
    if warehouse_type == "redshift":
        return ref_name.lower()
    return ref_name
