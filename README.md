# covidobserve

Main structure
Pipeline.py is the main script of 2 main functions. Each stg function is of 5 step functions.

    Stg1:
        Step 1. Filter by Location
        Step 2. Tweet Hydration (ID to full Tweet JSON)
        Step 3. Filter by English
        Step 4. NER tagging
        Step 5. Get graphs
    Stg2:
        Step 1. Get peaking entities
        Step 2. Trace back to texts.
        Step 3. Get nouns and noun-phrases.
        Step 4. Get KeyGraph.
        Step 5. Detect community and group the n&nps.

Use Stg1() to process individual CrisisNLP files; Stg2() to process an entire folder after individual files are created.

Data flow
CrisisNLP raw data per day (only provide geo data and Tweet ID), dict
↓
Tweet IDs filtered by geo data, json file of [ID]
↓
full Tweet JSON given by Twitter, json file
↓
Tweet ID and full_text property (full_text + retweet full_text if it’s a retweet), tuple (ID, full_text) 
↓
NER (Name entity recognition) tagged entities, list [NER entity] 
↓
Graph, dict

After graphs are collected under graph folder:
Graphs
↓
Peaking entities
↓
Full_texts, traced back to
↓
Nouns and noun-phrases, extracted from full_texts
↓
    KeyGraph
↓
Community of nouns and noun-phrases

File based explanation
Preprocessing.py
texts2NER() is the main function. Use pipeline_config.py to decide what NER types to include/exclude. This is where “Organization”, “People” and “Place” filtering is decided.
get_NERs() is where Stanza NER tagging takes place. GPU is automatically enabled via use_gpu. Input is a large string of tweets joined by delimiter. Delimiter to be configured in pipeline_config.py.

Graph_building.py
get_knowledge_graph() is the main function. Preprocessing step (texts2NER) is included. Can be skipped if input data is NERs or docs. Graph data is of properties “e_sigs_mean”, which is a python dictionary of entity significance, “edge_weight” (as illustrated in the paper), “timeblock”, “word_index_dict” for full_text tracing, and “doc_length” for divide2blocks() operation in time_series_analysis.py.

Time_series_analysis.py
get_peaking_entities() is the main func. The process involves: removing trending entities with remove_trend(), evaluation per mean and standard deviation according to paper



and eventually, removing those with significance below minimum. Configure X, Y, minimum from stg2(). The paper didn’t specify the trending removal process, hence remove_trend() is my own creation based on manipulation of first differences. Feel free to edit this function to your content.

Topic_summarization.py
get_key_graph() and get_groups() are the main funcs for this file.

The essential step, texts2docs() is moved to NNPextraction_ToPMine.py and NNPextraction_TextBlob.py. Import the function from either file.
get_key_graph() takes in a timeblock to get the corresponding file from the “noun and noun phrases” folder and to run get_knowledge_graph() on the file.
Get_groups takes in a graph and output groups of keywords belong to that graph.

NNPextraction_ToPMine.py
Extract n&nps with ToPMine algorithm. Configure the parameters with ToPMine/TopicalPhrases/run.sh. It’s able to extract longer and more distinctive phrases that help to understand the tweet better, however may include adverbs or verbs etc. Increase “thresh” under run.sh to reduce the selection pool. (4 is the code author’s default value)

NNPextraction_TextBlob.py
Extract n&nps with TextBlob algorithm. It’s more accurate in identifying nouns and noun phrases, but since the selected phrases are shorter, more common and less distinctive, it’s harder for Louvaine detection to find accurate ways of grouping them.
