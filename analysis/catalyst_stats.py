import pandas as pd, ast
from collections import Counter

df = pd.read_csv('data/ex_99_classified.csv')
total = len(df)
is_pr = df['is_pr'].sum()
not_pr = total - is_pr
has_title = df[df['is_pr']==True]['title'].notna().sum()
no_title = is_pr - has_title

TARGET = {"m&a", "biotech", "private_placement", "new_product"}

def parse_cats(val):
    try: return ast.literal_eval(val)
    except: return [val]

def is_other(val):
    return parse_cats(val) == ['other']

def is_target(val):
    return bool(set(parse_cats(val)) & TARGET)

pr = df[df['is_pr']==True]
target_count = pr['catalyst'].apply(is_target).sum()

cats = []
for val in pr['catalyst'].dropna():
    cats.extend(parse_cats(val))
c = Counter(cats)

print(f'Total exhibits : {total}')
print(f'Press releases : {is_pr}')
print(f'  with title   : {has_title}')
print(f'  no title     : {no_title}')
print(f'Not PR         : {not_pr}')
print(f'\nTarget PRs (price fetch): {target_count}')
print()
print('Catalyst breakdown (PRs only):')
for tag, count in c.most_common():
    marker = ' <-- target' if tag in TARGET else ''
    print(f'  {tag:<20} {count:>5}  ({count/is_pr*100:.1f}%){marker}')
