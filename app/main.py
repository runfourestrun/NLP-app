from neo4j import GraphDatabase
import os
from spacy.cli.download import download
from neo4j_batch import batch_parameters,ship_batch
from nlp import get_unique_nouns_and_verbs



def get_document(input_directory):
    '''
    Read documents in
    :param input_directory: string, path to input directory that contains text files.
    :return: all parameters: List of dicts. [{'id':filename,'text':text}]
    '''
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





def process_words_to_cypher_parameters(word_dict, word_type = None):
    '''
    Takes a word dict and marshalls them into the format expected for Neo4j Parameters in Cypher query.
    :param word_dict: dict{string:set}, example: {id: set{verbtext1,verbtext}}
    :param word_type: string,
    :return:
    '''
    parameters = []
    acceptable_word_types = ['noun','verb']
    if word_type.lower() in acceptable_word_types:
        for key,value in word_dict.items():
            for word in value:
                parameter = {'origin':key,'word':word,'type':word_type}
                parameters.append(parameter)
    return parameters



def create_document_in_neo(tx, batch):
    '''
    Mutator method. Creates node with Document label in Neo4j Database.
    :param tx: Neo4j Transaction
    :param batch: Batch of inputs for parameters. example: {'batch':[{k1,v1},{k2,v2}]} (generated by shipBatch method)
    :return:
    '''
    tx.run(
        '''
        UNWIND $batch as param
        CREATE (d:Document {id:param.id,text:param.text})
        ''', parameters=batch)



def read_document_in_neo(tx):
    '''
    Reads node with Document label in Neo4j database. Returns a list of results.
    :param tx: Neo4j Transaction
    :return: list(Neo4jRecord,Neo4jRecord)
    '''
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
    '''
    Writes nodes with Noun Label to Neo4j Database. Creates a Relationship to the source document.
    :param tx: Neo4j Transaction
    :param batch: Batch of inputs for parameters. example: {'batch':[{k1,v1},{k2,v2}]} (generated by shipBatch method)
    :return:
    '''
    tx.run(
        '''
        UNWIND $batch as param
        MATCH (d:Document{id:param.origin})
        MERGE (n:Noun {id:param.word}) <- [:CONTAINS] - (d)
        ''',parameters=batch)

def write_verbs(tx, batch):
    '''
    Writes nodes with Verb Label to Neo4j Database. Creates a Relationship to the source document.

    :param tx: Neo4j Transaction
    :param batch: Batch of inputs for parameters. example: {'batch':[{k1,v1},{k2,v2}]} (generated by shipBatch method)
    :return:
    '''
    tx.run(
        '''
        UNWIND $batch as param
        MATCH (d:Document{id:param.origin})
        MERGE (v:Verb {id:param.word}) <- [:CONTAINS] - (d)
        '''
    ,parameters=batch)



if __name__ == '__main__':

    '''
    Configuration
    '''


    input_directory = '/Users/alexanderfournier/PycharmProjects/NLP/documents'
    neo4j_uri = 'bolt://localhost:7687'
    neo4j_database = 'NLP'
    neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=('neo4j','changeme!'))



    document_parameters =  get_document(input_directory)
    document_batches = batch_parameters(document_parameters,500)


    final_document_batches = list(map(ship_batch,document_batches))

    '''
    BUG: For some reason I have to run the script twice. I don't think I understand how these sessions work.... 
    
    with neo4j_driver.session() as session:
        for batch in final_document_batches:
            session.write_transaction(create_document_in_neo,batch)
        session.close()
    '''








    with neo4j_driver.session() as session:
        records = session.read_transaction(read_document_in_neo)




        for record in records:
            unique_verbs, unique_nouns = get_unique_nouns_and_verbs(record)
            verb_parameters = process_words_to_cypher_parameters(unique_verbs, word_type='verb')
            noun_parameters = process_words_to_cypher_parameters(unique_nouns, word_type='noun')

            verb_batches = batch_parameters(verb_parameters, 500)
            noun_batches = batch_parameters(noun_parameters,500)

            final_verb_batches = list(map(ship_batch, verb_batches))
            final_noun_batches = list(map(ship_batch,noun_batches))

            for batch in final_verb_batches:
                session.write_transaction(write_verbs,batch)

            for batch in final_noun_batches:
                session.write_transaction(write_nouns,batch)

        session.close()
















































