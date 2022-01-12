import spacy
from neo4j import GraphDatabase
import os





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









if __name__ == '__main__':

    '''
    Configuration
    '''


    input_directory = '/Users/alexanderfournier/PycharmProjects/NLP/documents'
    neo4j_uri = 'bolt://localhost:7687'
    neo4j_database = 'NLP'
    neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=('neo4j','Reddit123!'))
    nlp = spacy.load('en_core_web_md')






    parameters =  get_document(input_directory)
    batches = batch_parameters(parameters,500)


    with neo4j_driver.session() as session:
        for _batch in batches:
            batch = {}
            batch['batch'] = _batch

            session.write_transaction(create_document_in_neo,batch)

        texts = session.read_transaction(read_document_in_neo)




















