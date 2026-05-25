import pandas as pd, ast, re, sys, io
from collections import Counter
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

df = pd.read_csv('data/ex_99_classified.csv')
pr = df[df['is_pr']==True].copy()

def is_other(val):
    try: return ast.literal_eval(val) == ['other']
    except: return val == 'other'

other = pr[pr['catalyst'].apply(is_other) & pr['title'].notna()]
print(f'Titled PRs in other: {len(other)}\n')

# print a sample
for t in other['title'].dropna().sample(min(80, len(other)), random_state=42):
    print(t)
