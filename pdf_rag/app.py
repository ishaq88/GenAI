import streamlit as st
from RAG import chat_response

st.set_page_config(
    page_title="PDF RAG",
    page_icon="🗝️",
)

st.write('###  RAG Application on Leave No Context Behind Paper')

query = st.text_input("What do you like to know about the paper?")
if query:
    with st.spinner('Thinking...'):
        st.write_stream(chat_response(query=query))