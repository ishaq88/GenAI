from langchain_chroma import Chroma
from langchain_community.document_loaders import UnstructuredPDFLoader
from langchain_community.embeddings.sentence_transformer import (
    SentenceTransformerEmbeddings,
)
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough

from langchain_groq import ChatGroq
import os

api_key = os.environ.get('GROQ_API_KEY')

file_path = "F:\innomatics\pdf_rag\paper.pdf"
data_file = UnstructuredPDFLoader(file_path)
docs = data_file.load()

splitter = RecursiveCharacterTextSplitter(chunk_size=800,
                                          chunk_overlap=100)
chunks = splitter.split_documents(docs)

embeddings = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")

vectorstore = Chroma.from_documents(chunks, embeddings)

vectorstore_retreiver = vectorstore.as_retriever(search_kwargs={"k": 10})

def chat_response(query: str):
    llm = ChatGroq(temperature=0, groq_api_key=api_key, model_name="llama3-8b-8192")

    template = """
    <|system|>>
    You are a helpful AI Assistant that follows instructions extremely well.
    Use the following context to answer user question.

    Think step by step before answering the question. You will get a $100 tip if you provide correct answer.

    CONTEXT: {context}
    </s>
    <|user|>
    {query}
    </s>
    <|assistant|>
    """

    prompt = ChatPromptTemplate.from_template(template)
    output_parser = StrOutputParser()

    chain = (
        {"context": vectorstore_retreiver, "query": RunnablePassthrough()}
        | prompt
        | llm
        | output_parser
    )

    for chunk in chain.stream(query):
         yield chunk