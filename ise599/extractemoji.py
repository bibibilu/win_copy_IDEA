import pandas as pd
import numpy as np

df = pd.read_json('C:/Users/Claire/IdeaProjects/ise599/tweets (1).json', lines=True)
a = df[df['lang'] == 'en']
