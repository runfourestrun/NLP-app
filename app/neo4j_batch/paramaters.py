def batch_parameters(parameters:list,batch_size:int):
    '''
    Chunks a list into smaller sublists. The idea here is to take create batches or chunks of parameters.
    :param parameters: input parameters
    :param chunk_size: size of sublists
    :return: list of lists. sublists contain a fixed number of elements (the last sublist will just contain the remainder)
    '''
    chunks = (parameters[x:x+batch_size] for x in range(0, len(parameters),batch_size))
    return chunks


def ship_batch(chunk):
    '''
    Parameters that are injected into Cypher must be a very specific data structure {'batch':[{k1,v1},{k2,v2}]}. All this function
    does is wrap the list of dicts in a parent dictionary with key 'batch'
    :param chunk: list(dict1,dict2). example: [{k1,v1},{k2,v2}]}
    :return: batch: {'batch':[{k1,v1},{k2,v2}]}
    '''
    batch = {}
    batch['batch'] = chunk
    return batch
