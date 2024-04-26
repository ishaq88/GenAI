# CineSage 

**[Check out the YouTube demo](https://www.youtube.com/watch?v=1jrV8hUSKtI)**

**Tech Stack: Langchain | ChromaDB | GroqAI | Llama | Streamlit**
****
**Have you ever been bursting with excitement after watching a fantastic movie or show, eager to discuss every detail and reference with someone who understands? But your friends and family just don't share your passion for cinema and are left scratching their heads at your enthusiastic ramblings?**

Well, Welcome to CineSage, your AI-powered movie buddy who's always ready to chat about anything and everything related to the world of films and series. CineSage is a massive cinephile who's seen it all, from the classics to the latest blockbusters, and is eager to delve into the intricacies of your favorite cinematic experiences.

### 🔍 [Hybrid search engine](https://github.com/ishaq88/GenAI/blob/main/CineSage%20-%20search%20engine%20%2B%20RAG%20application/hybrid_search.py) (Keyword + Semantic Search)

### 🧠 [RAG Application](https://github.com/ishaq88/GenAI/blob/main/CineSage%20-%20search%20engine%20%2B%20RAG%20application/RAG.py) with Chroma, LangChain, GroqAI, and Llama3

****
Check out the Data, Embeddings Ingestion techniques I employed while experimenting on how can i ingest huge data in ChromaDB,
just for reference i had around 450,000 docs which is only 1/4th of the original dataset of 82,400 subtitles files

### [Chunking, Embeddings, Ingestion using local computer, prefect for autoscheduling, one .txt file at a time](https://github.com/ishaq88/GenAI/tree/main/CineSage%20-%20search%20engine%20%2B%20RAG%20application/Data%20Ingestion(Prefect%20%2B%20text%20files)))
- Ran the prefect script processing 2000 files in a single batch every day, took 6-9 hours for processing one batch
- Cons: Very Slow, ran for 3 batches

### [Chunking, Embeddings, Ingestion using colab, pandas Dataframe](https://github.com/ishaq88/GenAI/tree/main/CineSage%20-%20search%20engine%20%2B%20RAG%20application/Data%20Ingestion(colab%20%2B%20pandas%20dataframe))
- Ran the chunking and embedding script on 1/4th files of original 82,400 files and got around 450,000 rows of chunks and embeddings out of this 1/4th files, took around 3hrs
- Ran the Ingestion in batches, 40,000 rows at a time took 10-12 minutes for a batch to be ingested into Chroma
