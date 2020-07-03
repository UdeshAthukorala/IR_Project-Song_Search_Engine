from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Index
import json,re,os
client = Elasticsearch(HOST="http://localhost",PORT=9200)
INDEX = 'song-index'


def createIndex():
    settings = {
        "settings": {
            "index":{
                "number_of_shards": "1",
                "number_of_replicas": "1"
            },
            "analysis" :{
                "analyzer":{
                    "sinhala-analyzer":{
                        "type": "custom",
                        "tokenizer": "icu_tokenizer",
                        "filter":["edge_ngram_custom_filter"]
                    }
                },
                "filter" : {
                    "edge_ngram_custom_filter":{
                        "type": "edge_ngram",
                        "min_gram" : 2,
                        "max_gram" : 50,
                        "side" : "front"
                    }
                }
            }
        },
        "mappings": {
            "properties": {
                    "title": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "song_lyrics": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "sinhala_artist": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "sinhala_lyrics": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "sinhala_music": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "sinhala_genre": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                        "analyzer" : "sinhala-analyzer",
                        "search_analyzer": "standard"
                    },
                    "english_artist": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                    },
                    "english_lyrics": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                    },
                    "english_music": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                    },
                    "english_genre": {
                        "type": "text",
                        "fields": {
                            "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                            }
                        },
                    },
                    "views": {
                        "type": "long",
                        # "fields": {
                        #     "keyword": {
                        #         "type": "keyword",
                        #         "ignore_above": 256
                        #     }
                        # },
                }
            }
        }
    }


    # index = Index(INDEX,using=client)
    # result = index.create()
    result = client.indices.create(index=INDEX , body =settings)
    print (result)


def read_translated_songs():
    THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
    my_file1 = os.path.join(THIS_FOLDER, 'data')
    my_file = os.path.join(my_file1, 'translated_songs.json')
    
    with open(my_file,'r',encoding='utf8') as tra_file:
        tra_songs = json.loads(tra_file.read())
        results_list = [a for num, a in enumerate(tra_songs) if a not in tra_songs[num + 1:]]
        return results_list


def clean_function(song_lyrics):
    if (song_lyrics):
        processed_list = []
        song_lines = song_lyrics.split('\n')
        
        for place,s_line in enumerate(song_lines):
            process_line = re.sub('\s+',' ',s_line)
            punc_process_line = re.sub('[.!?\\-]', '', process_line)
            processed_list.append(punc_process_line)
        
        sen_count = len(processed_list)
        final_processed_list = []
        
        for place,s_line in enumerate(processed_list):
            if (s_line=='' or s_line==' '):
                if (place!= sen_count-1 and (processed_list[place+1]==' ' or processed_list[place+1]=='')) :
                    pass
                else:
                    final_processed_list.append(s_line)
            else:
                final_processed_list.append(s_line)
        final_song_lyrics = '\n'.join(final_processed_list)
        return final_song_lyrics
    else:
        return None

def data_generation(song_array):
    for song in song_array:

        title = song["title"]
        song_lyrics = clean_function(song["song_lyrics"])
        views = song['views']

        sinhala_artist = song["sinhala_artist"]
        sinhala_lyrics = song["sinhala_lyrics"]
        sinhala_music = song["sinhala_music"]
        sinhala_genre = song["sinhala_genre"]
        
        english_artist = song["english_artist"]
        english_lyrics = song["english_lyrics"]
        english_music = song["english_music"]
        english_genre = song["english_genre"]
        

        yield {
            "_index": INDEX,
            "_source": {
                "title": title,
                "song_lyrics": song_lyrics,
                "views": views,
                "sinhala_artist": sinhala_artist,
                "sinhala_lyrics": sinhala_lyrics,
                "sinhala_music": sinhala_music,
                "sinhala_genre": sinhala_genre,
                "english_artist": english_artist,
                "english_lyrics": english_lyrics,
                "english_music": english_music,
                "english_genre": english_genre
            },
        }


createIndex()
translated_songs = read_translated_songs()
helpers.bulk(client,data_generation(translated_songs))