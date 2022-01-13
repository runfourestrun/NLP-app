import spacy
from neo4j import GraphDatabase
import os
from spacy.cli.download import download






def get_document(input_directory):
    all_parameters = []
    for root,directories,files in os.walk(input_directory):
        paths = [os.path.join(root,file) for file in files]
        for path in paths:
            filename = os.path.basename(path)
            with open(path) as fname:
                text = fname.read().replace('\n','')
                parameters = {'id':filename,'text':text}
                all_parameters.append(parameters)
    return all_parameters



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
    batch = {}
    batch['batch'] = chunk
    return batch



def get_unique_nouns_and_verbs(record):
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


def process_words_to_cypher_parameters(word_dict, word_type = None):
    parameters = []
    acceptable_word_types = ['noun','verb']
    if word_type.lower() in acceptable_word_types:
        for key,value in word_dict.items():
            for word in value:
                parameter = {'origin':key,'word':word,'type':word_type}
                parameters.append(parameter)
    return parameters



def create_document_in_neo(tx, batch):
    tx.run(
        '''
        UNWIND $batch as param
        CREATE (d:Document {id:param.id,text:param.text})
        ''', parameters=batch)



def read_document_in_neo(tx):
    results = []
    result = tx.run(
        '''
        MATCH (d:Document) where exists (d.text) return d
        '''
    )
    for record in result:
        results.append(record)
    return results





def write_nouns(tx, batch):
    tx.run(
        '''
        UNWIND $batch as param
        MERGE (n:Noun {id:param.word})
        MERGE (d:Document {id:param.origin}) - [:CONTAINS] -> (n)
        ''',parameters=batch)

def write_verbs(tx, batch):
    tx.run(
        '''
        UNWIND $batch as param
        MERGE (v:Verb {id:param.word})
        MERGE (d:Document {id:param.origin}) - [:CONTAINS] -> (v)
        '''
    ,parameters=batch)



if __name__ == '__main__':

    '''
    Configuration
    '''


    input_directory = '/Users/alexanderfournier/PycharmProjects/NLP/documents'
    neo4j_uri = 'bolt://localhost:7687'
    neo4j_database = 'NLP'
    neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=('neo4j','Reddit123!'))



    document_parameters =  get_document(input_directory)
    document_batches = batch_parameters(document_parameters,500)

    final_document_batches = list(map(ship_batch,document_batches))








    with neo4j_driver.session() as session:
        records = session.read_transaction(read_document_in_neo)
        for batch in final_document_batches:
            session.write_transaction(create_document_in_neo,batch)






    for record in records:
        unique_verbs,unique_nouns = get_unique_nouns_and_verbs(record)
        verb_parameters = process_words_to_cypher_parameters(unique_verbs,word_type='verb')
        noun_parameters = process_words_to_cypher_parameters(unique_nouns,word_type='noun')

        noun_batches = batch_parameters(noun_parameters,500)
        final_noun_batches = list(map(ship_batch,noun_batches))



        verb_batches = batch_parameters(verb_parameters,500)
        final_verb_batches = list(map(ship_batch,verb_batches))




        with neo4j_driver.session() as session:
            for batch in final_noun_batches:
                session.write_transaction(write_nouns,batch)










































