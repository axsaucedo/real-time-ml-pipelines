from html.parser import HTMLParser
import spacy
import dill
import re
from typing import List

nlp = spacy.load('en_core_web_sm', parser=False, entity=False)
html_parser = HTMLParser()
uplus_pattern = \
        re.compile("<[uU]\+(?P<digit>[a-zA-Z0-9]+)>")
markup_link_pattern = \
        re.compile("\[(.*)\]\((.*)\)")
symbols = set("!$%^&*()_+|~-=`{}[]:\";'<>?,./-")
tfidf_model_file = "models/tfidf_vectorizer.pickle"
tfidf_model = dill.load(open(tfidf_model_file, 'rb'))
lr_model_file = "models/lr_model.pickle"
lr_model = dill.load(open(lr_model_file, 'rb'))


async def transform_to_clean(raw_text: str) -> str:
    try:
        decoded = raw_text.encode("ISO-8859-1").decode("utf-8")
    except:
        decoded = raw_text.encode("ISO-8859-1").decode("cp1252")
    html_unescaped = html_parser.unescape(decoded) 
    html_unescaped = html_unescaped.replace("\r\n", " ")
    html_unescaped = uplus_pattern.sub(
        " U+\g<digit> ", html_unescaped)
    html_unescaped = markup_link_pattern.sub(
	" \1 \2 ", html_unescaped)
    html_unescaped = html_unescaped.replace("\\", "")
    html_unescaped = re.sub(r"\r\n", " ", html_unescaped)
    html_unescaped = html_unescaped.replace("&gt;", " > ")
    html_unescaped = html_unescaped.replace("&lt;", " < ")
    html_unescaped = html_unescaped.replace("--", " - ")
    markup_link_pattern.sub(
	" \1 \2 ", html_unescaped)
    return html_unescaped



async def transform_to_tokens(text: str) -> List[str]:
    doc = nlp(text, disable=['parser', 'tagger', 'ner'])
    tokens = []
    for token in doc:
        if token.like_url:
            clean_token = "URL"
        else:
            clean_token = str(token.lemma_.lower().strip())
            if len(clean_token) < 1 \
                    or clean_token in symbols: 
                continue
        tokens.append(clean_token)
    return tokens


def predict_mod_proba(tokens):
    tfidf_vectors = tfidf_model.transform([tokens])
    probability = lr_model.predict_proba(tfidf_vectors)[0,1]
    return (probability, tfidf_vectors)
    
