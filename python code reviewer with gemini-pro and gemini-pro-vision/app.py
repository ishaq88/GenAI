import google.generativeai as genai
import streamlit as st
import os
import PIL.Image

#gemini models configurations
GOOGLE_API_KEY=os.environ.get('GOOGLE_API_KEY')

genai.configure(api_key=GOOGLE_API_KEY)
model = genai.GenerativeModel('gemini-pro')
vision_model = genai.GenerativeModel('models/gemini-pro-vision')

safety = [
            {
                "category": "HARM_CATEGORY_DANGEROUS",
                "threshold": "BLOCK_NONE",
            },
            {
                "category": "HARM_CATEGORY_HARASSMENT",
                "threshold": "BLOCK_NONE",
            }]

generation_config = generation_config=genai.types.GenerationConfig(
                    max_output_tokens=500,
                    temperature=0.7
                    )

#streamlit app
st.title(":rainbow[Python Code Reviewer with Gemini]")
st.header(':rainbow[Multimodal (images and text)]', divider = 'rainbow')

#taking a text prompt
text_prompt = st.text_area("Enter your Python code to review here👇🏼:", height=200)

#or taking an image prompt
accepted_image_types = ['jpeg']
img_prompt = st.file_uploader('or upload an image containing the code', type=accepted_image_types)
 

if st.button("Review the Code"):
    st.divider()
    st.markdown("<h2 style='color:#ebff52; text-align:center;'>Code Review Result</h2>", unsafe_allow_html=True)
    
    # Combined text and image prompts
    if text_prompt and img_prompt:  
        img = PIL.Image.open(img_prompt)
        st.markdown("<p style='color:#254194; text-align:center;'>uploaded Image</p>", unsafe_allow_html=True)
        st.image(img)
        
        content = ["Review the given python code and image. Generate a list of mistakes in the code, and provide the fixed code by correcting the code. Consider the context from both the text and the image.", 
                    text_prompt, 
                    img]
        
        
        response = vision_model.generate_content(
            contents=content,
            safety_settings=safety, 
            generation_config=generation_config
        )
        
        response.resolve()
        generated_text = response.text
        st.write(generated_text)

    elif text_prompt:
        messages = [{"role": "user", 
                    "parts": ["Review the given python code and Generate what are the list of mistakes in the code and give fixed code by correcting the code"
                                        , text_prompt]}]
        
        response = model.generate_content(
            contents=messages,
            safety_settings=safety,
            generation_config=generation_config
        )
        generated_text = response.text
        st.write(generated_text)
    
    elif img_prompt:
        
        img = PIL.Image.open(img_prompt)
        st.markdown("<p style='color:#cfe2f3; text-align:center;'>uploaded Image</p>", unsafe_allow_html=True)
        st.image(img)
        response = vision_model.generate_content(["Review the given image. Generate a list of mistakes in the code, and provide the fixed code by correcting the code. Consider the context from the image.", img])
        response.resolve()
        generated_text = response.text
        st.write(generated_text)

    else:
        st.write("Please provide either code or an image for review.")
    