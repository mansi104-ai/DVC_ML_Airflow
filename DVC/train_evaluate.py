
import pandas as pd    # to load dataset
import numpy as np   
import nltk 
from nltk.corpus import stopwords  # for mathematic equation
from sklearn.model_selection import train_test_split       # for splitting dataset
from tensorflow.keras.preprocessing.text import Tokenizer  # to encode text to int
from tensorflow.keras.preprocessing.sequence import pad_sequences   # to do padding or truncating
from tensorflow.keras.models import Sequential     # the model
from tensorflow.keras.layers import Embedding, LSTM, Dense # layers of the architecture
from tensorflow.keras.models import load_model   # load saved model
# import nltk
import json
from dvclive import Live
import json
import yaml
import sys 
import tensorflow as tf

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')

tf.keras.utils.set_random_seed(1)
tf.config.experimental.enable_op_determinism()

params = yaml.safe_load(open("params.yaml"))["train"]

if len(sys.argv) != 2:
    sys.stderr.write("Arguments error. Usage:\n")
    sys.exit(1)

data_file = sys.argv[1]

nltk.download('stopwords')

EMBED_DIM = 32
#LSTM_OUT = 8
english_stops = set(stopwords.words('english'))

def load_dataset():
    
    df = pd.read_csv(f'{data_file}')
    x_data = df['review'].astype(str)       # Reviews/Input
    y_data = df['sentiment']    # Sentiment/Output

    # PRE-PROCESS REVIEW
    x_data = x_data.replace({'<.*?>': ''}, regex = True)          # remove html tag
    x_data = x_data.replace({'[^A-Za-z]': ' '}, regex = True)     # remove non alphabet
    x_data = x_data.apply(lambda review: [w for w in review.split() if w not in english_stops])  # remove stop words
    x_data = x_data.apply(lambda review: [w.lower() for w in review])   # lower case
    
    # ENCODE SENTIMENT -> 0 & 1
    y_data = y_data.replace('Positive', 1)
    y_data = y_data.replace('Negative', 0)

    return x_data, y_data

def get_max_length():
    
    review_length = []
    for review in x_train:
        review_length.append(len(review))

    return int(np.ceil(np.mean(review_length)))


x_data, y_data = load_dataset()

x_train, x_test, y_train, y_test = train_test_split(x_data, y_data, test_size = 0.2, random_state=42)


token = Tokenizer(lower=False)    # no need lower, because already lowered the data in load_data()
token.fit_on_texts(x_train)
x_train = token.texts_to_sequences(x_train)
x_test = token.texts_to_sequences(x_test)

max_length = get_max_length()

x_train = pad_sequences(x_train, maxlen=max_length, padding='post', truncating='post')
x_test = pad_sequences(x_test, maxlen=max_length, padding='post', truncating='post')

total_words = len(token.word_index) + 1   # add 1 because of 0 padding



model = Sequential()
model.add(Embedding(total_words, EMBED_DIM, input_length = max_length))
model.add(LSTM(params['lstm_out']))
model.add(Dense(1, activation='sigmoid'))
model.compile(optimizer = 'adam', loss = 'binary_crossentropy', metrics = ['accuracy'])

history = model.fit(x_train, y_train, batch_size = 128, epochs = 5)
model.save('model.h5')

print(history.history['accuracy'])
print(history.history['loss'])
prc_points = list(zip(history.history['accuracy'], history.history['loss']))

with open('history.json', "w") as fd:
    json.dump(
        {
            "history": [
                {"accuracy": val, "epochs": i}
                for i, val in enumerate(history.history['accuracy'])
            ]
        },
        fd,
        indent=4,
    )


print('Evaluate model...')

loaded_model = load_model('model.h5')
loss,acc = loaded_model.evaluate(x_test, y_test, verbose=1)

with open('evaluation.json', "w") as fd:
    json.dump(
        {
            "test_accuracy": acc
        },
        fd,
        indent=4,
    )

