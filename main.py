import spacy
from spacy.symbols import *
nlp = spacy.load('en_core_web_md')
nlp.Defaults.stop_words |= {'tt', 'ak', 'tl', 'tr', 'skd', 'aw'}
from neo4j import GraphDatabase
from string import punctuation





def process_text(uri,driver,database,batchsize):
    with driver.session(database=database) as session:
        with session.begin_transaction() as tx:
            results = tx.run(query, {})
            batch = []
            total = 0
            for num, result in enumerate(results):
                terms = get_terms_keywords(result['r']['text'])
                batch.append({"id": result['r']['id'], "tp": terms})

                if len(batch) % batchsize == 0:
                    print("load batch", len(batch))
                    load_terms(batch)
                    total = total + len(batch)
                    print("total loaded:", total)
                    batch = []
            print("process leftover batch", len(batch))
            load_terms(batch)
            total = total + len(batch)
            print("total loaded:", total)



def load_terms(batch):
    with driver.session(database=database) as session:
        with session.begin_transaction() as tx:
            res = tx.run(load, rows=batch)


def get_terms_keywords(data):
    nps = []
    doc = nlp(data)
    for np in extract_keywords(nlp, data):
        nps.append({"index": 9999, "text": str(np), "pos": "np"})
    return {"terms": nps, "phrases": nps}


def extract_keywords(nlp, sequence, special_tags: list = None):
    """ Takes a Spacy core language model,
    string sequence of text and optional
    list of special tags as arguments.

    If any of the words in the string are
    in the list of special tags they are immediately
    added to the result.

    Arguments:
        sequence {str} -- string sequence to have keywords extracted from

    Keyword Arguments:
        tags {list} --  list of tags to be automatically added (default: {None})

    Returns:
        {list} -- list of the unique keywords extracted from a string
    """
    result = []

    # custom list of part of speech tags we are interested in
    # we are interested in proper nouns, nouns, and adjectives
    # edit this list of POS tags according to your needs.
    pos_tag = ['PROPN', 'NOUN', 'ADJ']

    # create a spacy doc object by calling the nlp object on the input sequence
    doc = nlp(sequence.lower())

    # if special tags are given and exist in the input sequence
    # add them to results by default
    if special_tags:
        tags = [tag.lower() for tag in special_tags]
        for token in doc:
            if token.text in tags:
                result.append(token.text)

    for chunk in doc.noun_chunks:
        final_chunk = ""
        for token in chunk:
            if (token.pos_ in pos_tag) and not token.is_stop:
                final_chunk = final_chunk + token.text + " "
        if final_chunk:
            result.append(final_chunk.strip())
    for token in doc:
        if (token.text in nlp.Defaults.stop_words or token.text in punctuation):
            continue
        if (token.pos_ in pos_tag):
            result.append(token.text)
    return list(set(result))





if __name__ == '__main__':
    read_query = """
    MATCH (r:Report) where exists (r.text) return r 
    """

    load_query = """
    UNWIND $rows as row
    MERGE (r:Report {id:row.id})
    FOREACH (term in row.tp.terms |
      MERGE (tf:Term {word:toLower(term.text)})
      MERGE (r)-[:HAS_TERM]->(tf)
    )
    """

    uri = "bolt://localhost:7687"
    driver = GraphDatabase.driver(uri, auth=("neo4j", "Reddit123!"))
    database = "NLP"
    batchsize = 4000
    process_text(uri,driver,database,batchsize)