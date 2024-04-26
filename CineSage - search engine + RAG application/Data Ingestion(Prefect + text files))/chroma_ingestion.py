from langchain_community.document_loaders import TextLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain.docstore.document import Document
from langchain_chroma import Chroma
from langchain_community.embeddings.sentence_transformer import (
    SentenceTransformerEmbeddings,
)
import re
import os
from datetime import time
import chromadb
import prefect
from prefect import flow, task
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

@task
def cleaning_subtitles_text(text):
    text = re.sub(r"<[^>]*>", "", text)  # Remove HTML tags
    text = re.sub(r"{[^}]*}", "", text)  # Remove curly braces content
    text = re.sub(r"\[.*?\]", "", text)  # Remove square brackets content
    text = re.sub(r"\([^)]*\)", "", text)  # Remove parentheses content
    text = re.sub(r"\d\d:\d\d:\d\d,\d\d\d --> \d\d:\d\d:\d\d,\d\d\d", "", text)  # Remove time stamps
    text = re.sub(r"\n\d+\n", "\n", text)  # Remove extra lines
    text = re.sub(r"\n\s*\n", "\n", text)  # Remove multiple newlines
    text = re.sub(r"[^\w\s]", "", text)  # Remove non-alphanumeric characters
    text = text.lower()  # Convert text to lowercase

    unicode_pattern = re.compile(r'\\u[0-9a-fA-F]{4}') #removing unicode sequences
    text = re.sub(unicode_pattern, '', text)
    
    return text
    
@task
def process_subtitle_file(filename, embeddings, collection):
    loader = TextLoader(filename, encoding='latin-1')
    docs = loader.load()
    text = docs[0].page_content

    # Cleaning steps to remove srt formatting
    text = cleaning_subtitles_text(text)

    #converting the cleaned text again into a langchain doc
    doc = Document(page_content=text)

    #chunking the document
    splitter = RecursiveCharacterTextSplitter(chunk_size=850, chunk_overlap=60)
    chunks = splitter.split_documents([doc])

    #converting doc into str for embedding
    texts_str = [chunk.page_content for chunk in chunks]  

    #generating embeddings
    chunk_embeddings = embeddings.embed_documents(texts_str)

    for i, chunk_embedding in enumerate(chunk_embeddings):
        # Store in Chroma collection
        collection.add(
            embeddings=[chunk_embedding],
            documents=[chunks[i].page_content],
            ids=[f"{filename}_chunk{i}"]
        )


@task
def get_last_processed_batch(storage_path):
    try:
        with open(storage_path, "r") as f:
            last_line = f.readlines()[-1]  # Read the last line
            last_batch = int(last_line.strip())
    except FileNotFoundError:
        last_batch = -1 
    return last_batch

@task
def update_last_processed_batch(storage_path, batch_num):
    with open(storage_path, "a") as f: 
        f.write(f"\\n{batch_num}")
        
    print(f'processed batch {batch_num}')


@flow(name="chroma embeddings ingestion flow", log_prints=True)
def ingestion():
    # Initialize Embeddings object
    embeddings_func = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")

    # Initialize Chroma client and collection
    chroma_client = chromadb.PersistentClient(path='./chroma_db')
    collection = chroma_client.get_or_create_collection(name="subtitles")

    # Get list of subtitle file paths
    file_paths = [
        os.path.join("subtitles", filename)
        for filename in os.listdir("subtitles")
        if filename.endswith(".txt")
    ]
    
    
    # Process in batches of 2000 files at a time
    batch_size = 2000
    storage_path = 'F:/innomatics/search engine project/batch_processing_tab.txt'
    last_processed_batch = get_last_processed_batch(storage_path)
    start_index = (last_processed_batch + 1) * batch_size
    print(f'processing from the file index {start_index}')
    
    batch_paths = file_paths[start_index : start_index + batch_size]
    print(f"Processing batch {last_processed_batch + 1}...")
    
    for i, file_path in enumerate(batch_paths):
        process_subtitle_file(file_path, embeddings_func, collection)

    update_last_processed_batch(storage_path, str(last_processed_batch + 1))        
        
if __name__ == '__main__':
    ingestion.serve(
        name='chroma-embeddings-ingestion-pipeline',
        cron="0 */8 * * *", #running one batch every 8 hrs
    )