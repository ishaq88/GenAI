import chromadb
from langchain_chroma import Chroma
from datetime import datetime
from langchain_community.embeddings.sentence_transformer import (
    SentenceTransformerEmbeddings,
)
from langchain_community.retrievers import BM25Retriever

def hybrid_search(query: str):
  chroma_client = chromadb.PersistentClient(path='F:/innomatics/search engine project/vector_db')

  embeddings = SentenceTransformerEmbeddings(model_name="sentence-transformers/all-MiniLM-L6-v2")

  langchain_chroma = Chroma(
      client=chroma_client,
      collection_name='subtitles_collection',
      embedding_function= embeddings
  )

  vector_retriever = langchain_chroma.as_retriever(search_kwargs={"k": 15})
  result = vector_retriever.invoke(query)

  #rerank using bm25 keyword based retriever
  bm25_retriever = BM25Retriever.from_documents(documents=result)
  top_5_docs = bm25_retriever.invoke(query, k=5)

  return result, top_5_docs

def search_results(query: str):
  """
  Cleans search results by removing duplicates based on document content.
  """
  vector_result, hybrid_result = hybrid_search(query)
  
  seen_content = set()
  cleaned_vector_result = []
  cleaned_hybrid_result = []

  # Clean vector_result
  for doc in vector_result:
    if doc.page_content not in seen_content:
      cleaned_vector_result.append(doc)
      seen_content.add(doc.page_content)

  # Clean hybrid_result (we only want metadata for search engine)
  for doc in hybrid_result:
    if doc.metadata not in [d.metadata for d in cleaned_hybrid_result]:
      cleaned_hybrid_result.append(doc)

  return cleaned_vector_result, cleaned_hybrid_result

if __name__ == '__main__':
    
    start = datetime.now()
    vector_result, hybrid_result = hybrid_search('this is worse')
    end = datetime.now()
    print(f'query processed in {end - start} seconds')
    cleaned_vector_result, cleaned_hybrid_result = search_results(vector_result, hybrid_result)

    print(f'Retrieved Relevant Subtitles Files:')
    for doc in cleaned_hybrid_result:
        print(doc.metadata)