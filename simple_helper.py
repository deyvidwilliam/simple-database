def key_not_present(key, key_list, key_list2):
    """ Lookup for key in first element of the lists
        Function used in Write lock treatment (Read committed isolation level)
    """
    aux = [x for x in key_list if x[0] == key]
    aux2 = [x for x in key_list2 if x[0] == key]

    return len(aux) == 0 and len(aux2) == 0