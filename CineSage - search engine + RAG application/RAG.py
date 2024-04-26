from hybrid_search import search_results
from langchain_groq import ChatGroq
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_core.prompts import ChatPromptTemplate

import os
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime

api_key = os.environ.get('GROQ_API_KEY')
#api_key = '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'



def chat_response(query):
    """
    Generates a response to a user query using retrieved documents and an LLM.
    """
    # Get relevant documents and metadata
    vector_result, hybrid_result = search_results(query)

    # Extract document content and metadata for context
    context = ""
    for doc in hybrid_result:
        context += f"{doc.page_content}\n\nMetadata: {doc.metadata}\n\n"

    #prompt template
    template = """
    <|system|>
    You are a helpful, cheerful and playful AI Assistant that follows instructions extremely well.
    Your name is CineSage, your answer always start with quoting something catchy like famous dialogue from
    the movie that you have fetched in the context
    and you know everything related to cinema that is movies, shows etc.
    Use the following context to answer the user's question.
    if you have a context then look for metadata in the context string, it will be a dictionary in the format
    like 'filename' : filename, 'num' = num. now after first few catchy lines keep filename in your response
    with something like talking in context of 'filename'. 
    Think step-by-step before answering the question. 
    CONTEXT: {context}
    </s>
    <|user|>
    {query}
    </s>
    <|assistant|>
    """
    prompt = ChatPromptTemplate.from_template(template)

    # Initialize LLM and chain
    llm = ChatGroq(temperature=0, groq_api_key=api_key, model_name="llama3-8b-8192")
    output_parser = StrOutputParser()
    
    chain = (
        {'context': lambda x: context, 'query': RunnablePassthrough()}
        | prompt
        | llm
        | output_parser
    )
    
    for chunk in chain.stream(query):
         yield chunk
    

# Example usage

if __name__ == '__main__':
    user_query = "What is the relationship between Thor and Loki?"
    for chunk in chat_response(user_query):
        print(chunk, end="", flush=True)
