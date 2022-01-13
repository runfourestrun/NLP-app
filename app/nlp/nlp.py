import spacy

def get_unique_nouns_and_verbs(record):
    '''
    Using spacy to extract Nouns and Verbs from Neo4j Record
    :param record: Neo4j Record type - https://neo4j.com/docs/api/python-driver/current/api.html#record
    :return: tuple(dict{string:set}) example: you can unpack the tuple as follows: unique_nouns,unique_verbs = {id:set{verbtext1,verbtext2,verbtext3},{id2:set{nountext1,nountext2}}. id is the filename.
    '''
    for properties in record:
        text = properties.get('text')
        id = properties.get('id')
    nouns = []
    verbs = []
    #download(model='en_core_web_sm')
    nlp = spacy.load('en_core_web_sm')
    doc = nlp(text)
    for token in doc:
        if token.pos_ == 'NOUN':
            nouns.append(token.text)
        if token.pos_ == 'VERB':
            verbs.append(token.text)

    unique_nouns = {id:set(nouns)}
    unique_verbs = {id:set(verbs)}

    data = (unique_verbs,unique_nouns)
    return data