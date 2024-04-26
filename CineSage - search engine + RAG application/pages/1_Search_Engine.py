import streamlit as st
from hybrid_search import search_results

st.set_page_config(
    page_title="CineSage",
    page_icon="🗝️",
)

st.write('### Hybrid search engine (Keyword + Semantic Search)')

query = st.text_input("enter your subtitle query here")
if query:
    
    vector_result, hybrid_result = search_results(query=query)
    
    st.write('subtitle files recieved are ')
    for i, doc in enumerate(hybrid_result):
        st.markdown(f"{i+1}. [{doc.metadata['filename']}](https://www.opensubtitles.org/en/subtitles/{doc.metadata['num']})")
    
    st.write('The content from result is ')
    for doc in hybrid_result:
        st.write(doc.page_content)
        
