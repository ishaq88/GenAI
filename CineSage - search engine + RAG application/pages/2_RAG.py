import streamlit as st
from RAG import chat_response
from datetime import datetime

st.set_page_config(
    page_title="CineSage",
    page_icon="🗝️",
)

st.write('###  RAG Application with Chroma, LangChain, Groq, and Llama3')

query = st.text_input("ask me anything about movies and videos in general")
if query:
    with st.spinner('Thinking...'):
        st.write_stream(chat_response(query=query))