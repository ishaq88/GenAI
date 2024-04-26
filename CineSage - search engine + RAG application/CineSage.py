import streamlit as st

st.set_page_config(
    page_title="CineSage",
    page_icon="🗝️",
)


st.title("Welcome to CineSage: Your Cinephile friend! 🎬")

st.markdown(
    """
**Have you ever been bursting with excitement after watching a fantastic movie or show, eager to discuss every detail and reference with someone who understands? But your friends and family just don't share your passion for cinema and are left scratching their heads at your enthusiastic ramblings?**

Well, fret no more! Welcome to CineSage, your AI-powered movie buddy who's always ready to chat about anything and everything related to the world of films and series. CineSage is a massive cinephile who's seen it all, from the classics to the latest blockbusters, and is eager to delve into the intricacies of your favorite cinematic experiences.

Here's what CineSage offers:

### 🔍 [Hybrid search engine](http://localhost:8501/Search_Engine) (Keyword + Semantic Search)
- Head over to the Search Engine page to explore our powerful hybrid search engine. It combines the best of both worlds: keyword-based search and semantic search. This means you can find relevant scenes and dialogues from a vast collection of subtitles using either specific keywords or by describing the content in your own words. CineSage will understand your intent and retrieve the most relevant results, even if you don't remember the exact words!

### 🧠 [RAG Application](http://localhost:8501/RAG) with Chroma, LangChain, Groq, and Llama3
- Looking for something more conversational? Visit the RAG page to experience our Retrieval Augmented Generation (RAG) application. Powered by the combined forces of Chroma, LangChain, Groq, and the Llama3 language model, CineSage can engage in dynamic conversations about movies and shows. Ask questions about plot details, character relationships, thematic analysis, or anything else that comes to mind, and CineSage will provide insightful and informative responses based on its vast knowledge of cinema.
CineSage is your one-stop shop for all things cinematic. So, grab your popcorn, settle in, and get ready to chat with your new AI movie companion!
"""
)


